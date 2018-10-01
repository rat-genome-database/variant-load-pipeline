package edu.mcw.rgd.ratcn.fixup;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.ratcn.UpdateVariantStatus;
import edu.mcw.rgd.util.Zygosity;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 11/3/14
 * Time: 4:01 PM
 * <p>
 */
public class ZygosityPercReadFixUp {

    static public void main(String[] args) throws Exception {
        new ZygosityPercReadFixUp().run(729);
    }

    long linesProcessed = 0;
    long zygosityFixed = 0;
    long zygPercReadFixed = 0;
    long zygNumAllelesFixed = 0;
    Connection conn;
    PreparedStatement ps;

    void run(int sampleId) throws Exception {

        linesProcessed = 0;
        zygosityFixed = 0;
        zygPercReadFixed = 0;
        zygNumAllelesFixed = 0;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<String> samples = new TreeSet<String>();

        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        conn = ds.getConnection();

        ps = conn.prepareStatement("UPDATE variant SET zygosity_status=?,zygosity_percent_read=?,zygosity_num_allele=? "+
            "WHERE variant_id=?");

        String sql2 = "SELECT /*+ FIRST_ROWS */ v.variant_id, v.chromosome, v.start_pos, v.ref_nuc, v.var_nuc, " +
                "v.var_freq, v.zygosity_status, v.zygosity_percent_read, v.zygosity_num_allele FROM variant v WHERE v.sample_id=?"+
                " and zygosity_percent_read<>var_freq and variant_type='snv' order by chromosome,start_pos";
        PreparedStatement ps = conn.prepareStatement(sql2);
        ps.setInt(1, sampleId);
        ResultSet rs = ps.executeQuery();
        int lastPos = -1;
        List<Record> alleles = new ArrayList<Record>();

        while( rs.next() ) {
            Record r = new Record();
            r.variantId = rs.getLong(1);
            r.chr = rs.getString(2);
            r.pos = rs.getInt(3);
            r.refNuc = rs.getString(4);
            r.varNuc = rs.getString(5);
            r.varFreq = rs.getInt(6);
            r.zygosityStatus = rs.getString(7);
            r.zygosityPercentRead = rs.getInt(8);
            r.zygosityNumAllele = rs.getInt(9);

            if( r.pos>lastPos ) {
                qcAlleles(alleles);
                lastPos = r.pos;
            }
            alleles.add(r);

            linesProcessed++;
            if( linesProcessed%100000==0 ) {
                System.out.println(linesProcessed+" ["+(sdf.format(new Date()))+"]");
                System.out.println("   zygosity status fixed for "+zygosityFixed+" lines");
                System.out.println("   zygosity num alleles fixed for "+zygNumAllelesFixed+" lines");
                samples.clear();
            }
        }
        qcAlleles(alleles);

        ps.close();
        conn.close();

        System.out.println(linesProcessed+" ["+(sdf.format(new Date()))+"]");
        System.out.println("   zygosity status fixed for "+zygosityFixed+" lines");
        System.out.println("   zygosity num alleles fixed for "+zygNumAllelesFixed+" lines");
        System.out.println("OK!");
    }

    void qcAlleles(List<Record> alleles) throws SQLException {

        for( Record r: alleles ) {
            int zygosityNumAllele = alleles.size();
            if( zygosityNumAllele!=r.zygosityNumAllele ) {
                r.zygosityNumAllele = zygosityNumAllele;
                zygNumAllelesFixed++;
            }

            r.zygosityPercentRead = r.varFreq;

            String zygosityStatus = getZygosity(r.zygosityPercentRead);
            if( !Utils.stringsAreEqual(zygosityStatus, r.zygosityStatus) ) {
                r.zygosityStatus = zygosityStatus;
                zygosityFixed++;
            }

            updateAllele(r);
        }
        alleles.clear();
    }

    void updateAllele(Record r) throws SQLException {

        ps.setString(1, r.zygosityStatus);
        ps.setInt(2, r.zygosityPercentRead);
        ps.setInt(3, r.zygosityNumAllele);
        ps.setLong(4, r.variantId);
        ps.executeUpdate();

        System.out.println(" VID="+r.variantId+" c"+r.chr+":"+r.pos+" "+r.refNuc+"->"+r.varNuc);
    }

    String getZygosity(int percRead) {
        if( percRead== Zygosity.HOMOZYGOUS_PERCENT )
            return Zygosity.HOMOZYGOUS;
        else if( percRead>= Zygosity.POSSIBLY_HOMOZYGOUS_PERCENT )
            return Zygosity.POSSIBLY_HOMOZYGOUS;
        else {
            return Zygosity.HETEROZYGOUS;
        }
    }

    class Record {
        public long variantId;
        public String chr;
        public int pos;
        public String refNuc;
        public String varNuc;
        public int varFreq;
        public String zygosityStatus;
        public int zygosityPercentRead;
        public int zygosityNumAllele;
    }
}

