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
 * one time tool to (rcompute variant type
 */
public class VariantTypeFixUp {

    static public void main(String[] args) throws Exception {

        long linesProcessed = 0;
        long linesUpToDate = 0;
        long snvCount = 0;
        long insCount = 0;
        long delCount = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<String> samples = new TreeSet<String>();

        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        Connection conn = ds.getConnection();
        String sql = "SELECT v.variant_id, v.ref_nuc, v.var_nuc, v.variant_type, v.sample_id FROM variant v";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            long varId = rs.getLong(1);
            String refNuc = rs.getString(2);
            String varNuc = rs.getString(3);
            String varTypeInDb = rs.getString(4);
            String sampleId = rs.getString(5);

            // determine variant type
            String varTypeComputed = null;
            if( refNuc.contains("-") || varNuc.contains("-") ) {
                varTypeComputed = "del";
                delCount++;
            } else if( refNuc.length() < varNuc.length() ) {
                varTypeComputed = "ins";
                insCount ++;
            } else if( refNuc.length() > varNuc.length() ) {
                varTypeComputed = "del";
                delCount++;
            } else if( refNuc.length()==1 && varNuc.length()==1 ) {
                varTypeComputed = "snv";
                snvCount++;
            } else {
                System.out.println("Unknown variant type!");
            }

            samples.add(sampleId);

            if( Utils.stringsAreEqual(varTypeComputed, varTypeInDb) ) {
                linesUpToDate++;
            } else {
                updateVariantType(varId, varTypeComputed, ds);
            }
            linesProcessed++;

            if( linesProcessed%100000==0 ) {
                System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", SNV="+snvCount+", INS="+insCount+", DEL="+delCount
                        +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                        +" ["+(sdf.format(new Date()))+"]");
                samples.clear();
            }
        }
        updateVariantType(0, null, ds);

        conn.close();

        System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", SNV="+snvCount+", INS="+insCount+", DEL="+delCount
                +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                +" ["+(sdf.format(new Date()))+"]");
        System.out.println("OK!");
    }

    static List updateBatch = new ArrayList();
    static String sqlUpdateVariantType = "UPDATE variant SET variant_type=? WHERE variant_id=?";
    static int[] argTypes = new int[]{Types.VARCHAR, Types.INTEGER};

    static void updateVariantType(long varId, String varType, DataSource ds) throws Exception {
        updateBatch.add(new Object[]{varType, varId});

        boolean commitBatch = varId==0 || updateBatch.size()>=10000;

        if( commitBatch ) {
            new JdbcTemplate(ds).batchUpdate(sqlUpdateVariantType, updateBatch, argTypes);
            updateBatch.clear();
        }
    }
}
