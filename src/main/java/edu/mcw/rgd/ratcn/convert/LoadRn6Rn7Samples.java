package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.process.mapping.MapManager;
import edu.mcw.rgd.ratcn.VcfToCommonFormat2Converter;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LoadRn6Rn7Samples {

    public static void main(String[] args) throws Exception {
        try {
            int mapKey = 360; // rn6
            //int mapKey=372; // rn7
            // createSamples(mapKey);
            // convertToCommonFormat(mapKey);
        //    loadVariants(mapKey);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void convertToCommonFormat(int mapKey) throws Exception {
        SampleDAO sdao = new SampleDAO();
        sdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
        Connection conn = sdao.getDataSource().getConnection();

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        VcfToCommonFormat2Converter converter = (VcfToCommonFormat2Converter) bf.getBean("vcf2commonFormat2");


        String suffix = "_SNPs_HF_SnpEff.vcf.gz";
        File dir = new File("/data/rn6");
        if (mapKey==360) {
            suffix = "_HF_SnpEff.vcf.gz";
            dir = new File("/data/rn6");
        } else if( mapKey==372 ){
            suffix = "_HF_PASS_SnpEff.vcf.gz";
            dir = new File("/data/rnBN7");
        } else {
            throw new Exception("unexpected map_key="+mapKey);
        }

        File[] files = dir.listFiles();
        for (File f : files) {
            String fname = f.getName();
            if (f.isFile() && fname.endsWith(suffix)) {
                String sampleName = fname.substring(0, fname.length() - suffix.length());
                int sampleId = getSampleId(sampleName, conn);
                System.out.println(sampleId+" "+sampleName);

                List<String> argList = new ArrayList<>();
                argList.add("--vcfFile");
                argList.add(f.getAbsolutePath());
                argList.add("--outDir");
                argList.add(dir.getAbsolutePath());
                argList.add("--mapKey");
                argList.add(mapKey+"");
                argList.add("--compressOutputFile");
                argList.add("--appendToOutputFile");

                String[] args = argList.toArray(new String[0]);
                converter.parseCmdLine(args);

                converter.run();
            }
        }
    }

    static int getSampleId(String analysisName, Connection conn) throws SQLException {
        String sql = "SELECT sample_id FROM sample WHERE analysis_name=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, analysisName);
        ResultSet rs = ps.executeQuery();
        int sampleId = 0;
        while( rs.next() ) {
            if( sampleId!=0 ) {
                throw new SQLException("multi sample names");
            }
            sampleId = rs.getInt(1);
        }
        ps.close();
        return sampleId;
    }

    static void createSamples(int mapKey) throws Exception {
        SampleDAO sdao = new SampleDAO();
        sdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());

        Connection conn = sdao.getDataSource().getConnection();

        int sampleId = 1000;
        String suffix = "_SNPs_HF_SnpEff.vcf.gz";
        File dir = new File("/data/rn6");
        int patientId = 600;
        String gender = "U";
        String description = "rn6";
        if( mapKey==360 ) {
            sampleId = 1000;
            suffix = "_SNPs_HF_SnpEff.vcf.gz";
            dir = new File("/data/rn6");
            patientId = 600;
            gender = "U";
            description = "rn6";
        } else if( mapKey==372 ){
            sampleId = 3000;
            suffix = "_SNPs_HF_PASS_SnpEff.vcf.gz";
            dir = new File("/data/rnBN7");
            patientId = 372;
            gender = "U";
            description = "rnBN7";
        } else {
            throw new Exception("unexpected map_key="+mapKey);
        }

        File[] files = dir.listFiles();
        for( File f: files ) {
            String fname = f.getName();
            if( f.isFile() && fname.endsWith(suffix) ) {
                String sampleName = fname.substring(0, fname.length()-suffix.length());
                String sql = "INSERT INTO sample (sample_id,analysis_name,description,patient_id,gender,map_key,analysis_time) VALUES(?,?,?,?,?,?,SYSDATE)";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setInt(1, sampleId);
                ps.setString(2, sampleName);
                ps.setString(3, description);
                ps.setInt(4, patientId);
                ps.setString(5, gender);
                ps.setInt(6, mapKey);
                ps.executeUpdate();
                ps.close();

                sampleId++;
            }
        }

        conn.close();
    }
}
