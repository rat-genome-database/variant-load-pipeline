package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.dao.impl.GWASCatalogDAO;
import edu.mcw.rgd.dao.spring.XmlBeanFactoryManager;
import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;

import javax.sql.DataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class GwasCat2Vcf {

    GWASCatalogDAO dao = new GWASCatalogDAO();
    static final int refCount = 8;
    static final int varCount = 1;

    public static void main(String[] args) throws Exception{

        List<Integer> mapKeys = new ArrayList<>();
        List<String> outputFiles = new ArrayList<>();

        for( int i=0; i<args.length; i++ ) {
            switch( args[i] ) {
                case "--mapKey":
                    mapKeys.add(Integer.parseInt(args[++i]));
                    break;

                case "--outputFile":
                    outputFiles.add(args[++i]);
                    break;
            }
        }
        GwasCat2Vcf convert = new GwasCat2Vcf();
        for (int i = 0; i <mapKeys.size(); i++){
            convert.run(mapKeys.get(i),outputFiles.get(i));
        }
    }
    void run(int mapKey, String outputFile) throws Exception{

        System.out.println("GWASCat2VCF Started");
        System.out.println("  --mapKey     "+mapKey);
        System.out.println("  --outputFile "+outputFile);

        BufferedWriter writer = openOutputFile(outputFile);
        writer.write("##fileformat=VCFv4.1\n");
        writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tGWASCATALOG"+mapKey+"\n");

        List<GWASCatalog> gwas = dao.getFullCatalog(); // change bean in xml file

        for (GWASCatalog gc : gwas) {
            String allele;
            if ( gc.getChr()!=null && gc.getStrongSnpRiskallele()!=null) {
                allele = getRefAllele(mapKey, gc);
                if (gc.getStrongSnpRiskallele().equals("?"))
                    continue;
                writeVcfLine(gc,allele,writer);
            }
        }
        writer.close();
    }
    String getRefAllele(int mapKey, GWASCatalog gc) throws Exception {

        FastaParser parser = new FastaParser();
        parser.setMapKey(mapKey);
        if( parser.getLastError()!=null ) {

        }

        parser.setChr(Utils.defaultString(gc.getChr()));

        int startPos = Integer.parseInt(Utils.defaultString(gc.getPos()));
        int stopPos = Integer.parseInt(Utils.defaultString(gc.getPos()));

        String fasta = parser.getSequence(startPos, stopPos);
        if( parser.getLastError()!=null ) {

        }
        if( fasta == null ) {
            return null;
        }

        return fasta;
    }

    BufferedWriter openOutputFile(String outputFile) throws IOException {
        if( outputFile.endsWith(".gz") ) {
            return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputFile))));
        } else {
            return new BufferedWriter(new FileWriter(outputFile));
        }
    }

    ArrayList<GWASCatalog> getCatalog() throws Exception{
        ArrayList<GWASCatalog> gwas = new ArrayList<>();

        return gwas;
    }

    void writeVcfLine(GWASCatalog gc, String ref, BufferedWriter writer) throws IOException {

        //chrom
        writer.write(gc.getChr());

        //pos
        writer.write("\t"+gc.getPos());

        //id
        writer.write("\t"+gc.getSnps());

        //ref
        writer.write("\t"+ref);

        //alt risk allele
        String varNuc = gc.getStrongSnpRiskallele().replaceAll("\\s+","");
        writer.write("\t"+varNuc);

        // QUAL
        writer.write("\tPASS");

        // FILTER
        writer.write("\tVALIDATED=1");

        //info
        writer.write("\t");

        //format

        writer.write("\tGT;AD;DP");

        writer.write("\t0/1:"+refCount+","+varCount+":"+(refCount+varCount));

        writer.write("\n");

    }

    DataSource getDataSource() {
        return (DataSource) (XmlBeanFactoryManager.getInstance().getBean("DataSource"));
    }
}
