package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.Utils;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Types;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 7/8/15
 * Time: 3:21 PM
 * <p>
 * load conservation scores from UCSC
 * phastCons File Format:
  When uncompressed, the file contains a declaration line and one column of data in wiggle table fixed-step format:

  fixedStep chrom=scaffold_1 start=3462 step=1
  0.0978
  0.1588
  0.1919

  1. Declaration line: specifies starting point of the data in the assembly.
    It consists of the following fields:

    fixedStep -- keyword indicating the wiggle track format used to write the data.
                 In fixed step format, the data is single-column with a fixed interval between values.
    chrom -- chromosome or scaffold on which first value is located.
    start -- position of first value on chromosome or scaffold specified by chrom.
             NOTE: Unlike most Genome Browser coordinates, these are one-based.
    step -- size of the interval (in bases) between values.

  A new declaration line is inserted in the file when the chrom value changes, when a gap is encountered
 (requiring a new start value), or when the step interval changes.
 *
  2. Data lines
 * </p>
 */
public class ConservationScoreLoader {

    private String tableName;
    private String dataSourceName = "Carpe";

    public static void main(String[] args) throws Exception {

        String fileName = null;
        String tableName = null;
        String dataSourceName = null;

        for( int i=0; i<args.length; i++ ) {
            switch(args[i]) {
                case "--fileName":
                    fileName = args[++i];
                    break;
                case "--tableName":
                    tableName = args[++i];
                    break;
                case "--dataSourceName":
                    dataSourceName = args[++i];
                    break;
            }
        }

        if( tableName==null || fileName==null ) {
            System.out.println("Conservation Score Loader ver. 1.0\n");
            System.out.println("   --fileName <name of gzipped file with phastCons scores in wig format>");
            System.out.println("   --tableName <name of table the data is to be put into>");
            System.out.println("   --dataSourceName <optional data source name -- default is CarpenovoDataSource>");
            System.exit(-1);
        }

        ConservationScoreLoader loader = new ConservationScoreLoader();
        loader.run(fileName, tableName, dataSourceName);
    }

    public void run(String fname, String tableName, String dataSourceName) throws Exception {

        if( dataSourceName!=null ) {
            this.dataSourceName = dataSourceName;
        }
        this.tableName = tableName;

        System.out.println("File to load: "+fname);
        System.out.println("Data source name: "+this.dataSourceName);
        System.out.println("Table name: "+tableName);

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fname))));
        String line;
        long dataLines = 0, ctrlLines = 0, badChrLines = 0;
        int pos = 0, step = 0;
        String chr = "";
        Set<String> chromosomes = new TreeSet<>();
        while( (line=reader.readLine())!=null ) {

            if( line.startsWith("fixedStep") ) {
                String[] attrs = line.split("[\\s]+");
                for( String attr: attrs ) {
                    if( attr.startsWith("chrom=") ) {
                        chr = attr.substring(6);
                        if( chr.startsWith("chr") ) {
                            chr = chr.substring(3);
                        }
                        chromosomes.add(chr);
                    } else if( attr.startsWith("start=") ) {
                        pos = Integer.parseInt(attr.substring(6));
                    } else if( attr.startsWith("step=") ) {
                        step = Integer.parseInt(attr.substring(5));
                    }
                }
                ctrlLines++;
                continue;
            }

            if( chr.length()<=2 ) {
                ++dataLines;

                double score = Double.parseDouble(line);
                insertRow(chr, pos, score);
            } else {
                // skip unmapped contigs
                ++badChrLines;
            }

            if( (dataLines+ctrlLines+badChrLines)%1000000==0 ) {
                System.out.println("--- MILLION LINES LANDMARK ---");
                System.out.println("  - chromosomes: "+ Utils.concatenate(chromosomes, " "));
                System.out.println("  - dataLines=" + Utils.formatThousands(dataLines)
                        + "  ctrlLines=" + Utils.formatThousands(ctrlLines)
                        + "  badChrLines=" + Utils.formatThousands(badChrLines));
            }

            pos += step;
        }
        reader.close();

        insertRow(null, 0, 0.0); // flush the remaining data

        System.out.println("--- END OF FILE ---");
        System.out.println("OK " + " dataLines=" + Utils.formatThousands(dataLines)
                + "  ctrlLines=" + Utils.formatThousands(ctrlLines)
                + "  badChrLines=" + Utils.formatThousands(badChrLines));
        System.out.println("chromosomes: "+ Utils.concatenate(chromosomes, " "));
    }

    public final int BATCH_SIZE = 50000;
    List<Object[]> rowCache = new ArrayList<>(BATCH_SIZE);
    AbstractDAO dao = new AbstractDAO();

    public void insertRow(String chr, int pos, double val) throws Exception {
        if( chr==null ) {
            insertBatch();
        } else {
            rowCache.add(new Object[]{pos, chr, val});

            if( rowCache.size()>=BATCH_SIZE ) {
                insertBatch();
            }
        }
    }

    public void insertBatch() throws Exception {
        if( rowCache.isEmpty() )
            return;

        String sql = "INSERT INTO "+tableName+" (position,chr,score) VALUES(?,?,?)";
        BatchSqlUpdate su = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(dataSourceName), sql,
                new int[]{Types.INTEGER, Types.VARCHAR, Types.DOUBLE},
                BATCH_SIZE);
        su.compile();
        for( Object[] row: rowCache ) {
            su.update(row[0], row[1], row[2]);
        }
        dao.executeBatch(su);

        rowCache.clear();
    }
}
