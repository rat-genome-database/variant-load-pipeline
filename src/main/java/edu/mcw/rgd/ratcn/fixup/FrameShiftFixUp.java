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
 * Time: 4:01 PM
 * <p>
 * one time tool to recompute VARIANT_TRANSCRIPT.FRAMESHIFT field
 */
public class FrameShiftFixUp {

    static public void main(String[] args) throws Exception {

        long linesProcessed = 0;
        long linesUpToDate = 0;
        long linesFrameShiftT = 0;
        long linesFrameShiftF = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<String> samples = new TreeSet<String>();

        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        Connection conn = ds.getConnection();
        String sql = "SELECT /*+ FIRST_ROWS */ vt.variant_transcript_id, v.ref_nuc, v.var_nuc, v.sample_id, vt.frameshift "+
                "FROM variant v,variant_transcript vt WHERE v.variant_id=vt.variant_id";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {

            if( linesProcessed%100000==0 ) {
                System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", F="+linesFrameShiftF+", T="+linesFrameShiftT
                        +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                        +" ["+(sdf.format(new Date()))+"]");
                samples.clear();
            }

            long varTrId = rs.getLong(1);
            String refNuc = rs.getString(2);
            String varNuc = rs.getString(3);
            String sampleId = rs.getString(4);
            String frameShiftInDb = rs.getString(5);

            // determine frameshift
            int lenDiff;
            if( varNuc.contains("-") ) {
                lenDiff = refNuc.length();
            } else if( refNuc.contains("-") ) {
                lenDiff = varNuc.length();
            } else {
                lenDiff = Math.abs(refNuc.length()-varNuc.length());
            }
            String frameShiftComputed = lenDiff%3==0 ? "F" : "T";
            if( frameShiftComputed.equals("F") ) {
                linesFrameShiftF++;
            } else {
                linesFrameShiftT++;
            }

            samples.add(sampleId);

            if( Utils.stringsAreEqual(frameShiftComputed, frameShiftInDb) ) {
                linesUpToDate++;
            } else {
                updateFrameShift(varTrId, frameShiftComputed, ds);
            }
            linesProcessed++;
        }
        updateFrameShift(0, null, ds);

        conn.close();

        System.out.println(linesProcessed+". up-to-date="+linesUpToDate+", F="+linesFrameShiftF+", T="+linesFrameShiftT
                +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                +" ["+(sdf.format(new Date()))+"]");
        System.out.println("OK!");
    }

    static List updateBatch = new ArrayList();
    static String sqlUpdateFrameShift = "UPDATE variant_transcript SET frameshift=? WHERE variant_transcript_id=?";
    static int[] argTypes = new int[]{Types.VARCHAR, Types.INTEGER};

    static void updateFrameShift(long varTrId, String frameShift, DataSource ds) throws Exception {
        updateBatch.add(new Object[]{frameShift, varTrId});

        boolean commitBatch = varTrId==0 || updateBatch.size()>=10000;

        if( commitBatch ) {
            new JdbcTemplate(ds).batchUpdate(sqlUpdateFrameShift, updateBatch, argTypes);
            updateBatch.clear();
        }
    }
}


