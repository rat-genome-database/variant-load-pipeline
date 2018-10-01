package edu.mcw.rgd.ratcn.fixup;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.Utils;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 11/3/14
 * Time: 2:07 PM
 * <p>
 * one time tool to recompute VARIANT.ZYGOSITY_NUM_ALLELE
 */
public class NumAllelesFixUp {

    static public void main(String[] args) throws Exception {

        long linesProcessed = 0;
        long linesUpToDate = 0;
        long numAlleleUpdated = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<String> samples = new TreeSet<String>();

        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        Connection conn = ds.getConnection();
        String sql = "SELECT /*+ FIRST_ROWS */ v.variant_id, v.start_pos, v.zygosity_num_allele, v.zygosity_ref_allele, v.sample_id " +
                "FROM variant v "+
                "WHERE sample_id>=500 AND zygosity_percent_read>0 ORDER BY sample_id,chromosome,start_pos";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        int prevPos = 0;
        List<Long> varIds = new ArrayList<Long>();
        List<Integer> numAllelesInRgd = new ArrayList<Integer>();
        boolean zygosityRefAllele = false;

        while( rs.next() ) {
            long varId = rs.getLong(1);
            int pos = rs.getInt(2);
            int numAlleleInRgd = rs.getInt(3); // 1,2,...
            String refAllele = rs.getString(4); // 'Y','N',null
            String sampleId = rs.getString(5);
            samples.add(sampleId);

            if( pos!=prevPos ) {
                // pos change -- flush previous data
                int numAllelesComputed = varIds.size();
                if( zygosityRefAllele )
                    numAllelesComputed++;

                for( int i=0; i<varIds.size(); i++ ) {
                    int numAllele = numAllelesInRgd.get(i);
                    if( numAllele!=numAllelesComputed ) {
                        numAlleleUpdated++;
                        updateVariants(varIds.get(i), numAllelesComputed, ds);
                    } else {
                        linesUpToDate++;
                    }
                }
                varIds.clear();
                numAllelesInRgd.clear();
                prevPos = pos;
                zygosityRefAllele = false;
            }

            varIds.add(varId);
            numAllelesInRgd.add(numAlleleInRgd);
            if( Utils.stringsAreEqual(refAllele,"Y") )
                zygosityRefAllele = true;

            if( linesProcessed%1000000==0 ) {
                System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", num-alleles-updated="+numAlleleUpdated
                        +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                        +" ["+(sdf.format(new Date()))+"]");
                samples.clear();
            }
            linesProcessed++;
        }
        // pos change -- flush previous data
        int numAllelesComputed = varIds.size();
        if( zygosityRefAllele )
            numAllelesComputed++;
        for( int i=0; i<varIds.size(); i++ ) {
            int numAllele = numAllelesInRgd.get(i);
            if( numAllele!=numAllelesComputed ) {
                numAlleleUpdated++;
                updateVariants(varIds.get(i), numAllelesComputed, ds);
            } else {
                linesUpToDate++;
            }
        }
        // final flush
        updateVariants(0, 0, ds);

        conn.close();

        System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", num-alleles-updated="+numAlleleUpdated
                +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                +" ["+(sdf.format(new Date()))+"]");
        System.out.println("OK!");
    }

    static List updateBatch = new ArrayList();
    static String sqlUpdateVariantType = "UPDATE variant SET zygosity_num_allele=? WHERE variant_id=?";
    static int[] argTypes = new int[]{Types.INTEGER, Types.INTEGER};

    static void updateVariants(long varId, int numAllele, DataSource ds) throws Exception {
        //System.out.println(varId);
        updateBatch.add(new Object[]{numAllele, varId});

        boolean commitBatch = varId==0 || updateBatch.size()>=10000;

        if( commitBatch ) {
            new JdbcTemplate(ds).batchUpdate(sqlUpdateVariantType, updateBatch, argTypes);
            updateBatch.clear();
        }
    }
}
