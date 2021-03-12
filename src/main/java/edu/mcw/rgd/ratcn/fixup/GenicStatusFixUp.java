package edu.mcw.rgd.ratcn.fixup;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.ratcn.UpdateVariantStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 11/17/14
 * Time: 11:04 AM
 * <p>
 * validate VARIANT.GENIC_STATUS and update it if necessary
 */
public class GenicStatusFixUp {

    static public void main(String[] args) throws Exception {
        int startSampleId = Integer.parseInt(args[0]);
        int endSampleId = Integer.parseInt(args[1]);
        System.out.println("Genic status fix up for samples " + startSampleId + " - " + endSampleId);
        new GenicStatusFixUp().run(startSampleId, endSampleId);
    }

    MapDAO mapDAO = new MapDAO();

    void run(int startSampleId, int endSampleId) throws Exception {
        long linesProcessed = 0;
        long linesUpToDate = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Set<String> samples = new TreeSet<String>();

        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        Connection conn = ds.getConnection();

        UpdateVariantStatus variantStatusUpdater = new UpdateVariantStatus();

        String sql2 = "SELECT /*+ FIRST_ROWS */ v.variant_id, v.chromosome, v.start_pos, v.genic_status, v.sample_id FROM variant v "+
                "WHERE v.sample_id BETWEEN ? AND ?";
        PreparedStatement ps = conn.prepareStatement(sql2);
        ps.setInt(1, startSampleId);
        ps.setInt(2, endSampleId);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            long varId = rs.getLong(1);
            String chr = rs.getString(2);
            int pos = rs.getInt(3);
            String genicStatusInDb = rs.getString(4);
            String sampleId = rs.getString(5);
            int mapKey = getMapKey(sampleId, ds);

            // determine genic status
            String genicStatusComputed = getGenicStatus(mapKey, chr, pos);
            if( genicStatusComputed==null || Utils.stringsAreEqualIgnoreCase(genicStatusComputed, genicStatusInDb) ) {
                linesUpToDate++;
            } else {
                variantStatusUpdater.updateGenicStatus(genicStatusComputed.equals("genic"), varId, ds);
            }

            samples.add(sampleId);

            linesProcessed++;

            if( linesProcessed%100000==0 ) {
                System.out.println(linesProcessed+". up-to-date="+linesUpToDate+String.format(" (%.3f%%)",(100.0*linesUpToDate)/linesProcessed)
                        +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                        +" ["+(sdf.format(new Date()))+"]");
                samples.clear();
            }
        }
        variantStatusUpdater.commit(ds);

        conn.close();

        System.out.println(linesProcessed+". up-to-date="+linesUpToDate+String.format(" (%.3f%%)",(100.0*linesUpToDate)/linesProcessed)
                +" SAMPLES["+Utils.concatenate(samples,",")+"]"
                +" ["+(sdf.format(new Date()))+"]");
        System.out.println("OK!");
    }


    int getMapKey(String sampleId, DataSource ds) throws Exception {
        Integer mapKey = sampleIdToMapKeyMap.get(sampleId);
        if( mapKey==null ) {
            SampleDAO sampleDAO = new SampleDAO();
            sampleDAO.setDataSource(ds);
            mapKey = sampleDAO.getSample(Integer.parseInt(sampleId)).getMapKey();
            sampleIdToMapKeyMap.put(sampleId, mapKey);
        }
        return mapKey;
    }
    Map<String,Integer> sampleIdToMapKeyMap = new HashMap<String, Integer>();


    String getGenicStatus(int mapKey, String chr, int pos) throws Exception {
        return isGenic(mapKey, chr, pos) ? "genic" : "intergenic";
    }

    boolean isGenic(int mapKey, String chr, int pos) throws Exception {
        return !mapDAO.getMapDataWithinRange(pos, pos, chr, mapKey, 0).isEmpty();
    }
}
