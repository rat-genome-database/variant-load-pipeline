package edu.mcw.rgd.ratcn;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 3/10/14
 * Time: 12:38 PM
 * <p>
 * gene rgd ids searchable by position and chromosome
 */
public class TranscriptFeatureCache {

    HashMap<Integer,List<TranscriptFeatureCacheEntry>> result = new HashMap<Integer,List<TranscriptFeatureCacheEntry>>();
    public int loadCache(int mapKey, String chromosome, DataSource ds) throws SQLException {


        String sql = "SELECT tf.transcript_rgd_id,ro.OBJECT_NAME,md.STRAND, md.CHROMOSOME,md.START_POS,md.STOP_POS FROM\n" +
                "TRANSCRIPT_FEATURES tf inner join rgd_ids r on tf.FEATURE_RGD_ID = r.rgd_id \n" +
                "inner join maps_data md on md.MAP_KEY = ? AND md.CHROMOSOME = ? AND tf.FEATURE_RGD_ID = md.RGD_ID\n" +
                "inner join rgd_objects ro on r.OBJECT_KEY = ro.OBJECT_KEY  order by OBJECT_NAME, START_POS, STOP_POS";
        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, mapKey);
        ps.setString(2, chromosome);
        ResultSet rs = ps.executeQuery();

        while( rs.next() ) {
           int transcriptRgdId = rs.getInt(1);
            String objectName = rs.getString(2);
            String strand = rs.getString(3);
            String chr = rs.getString(4);
            int startPos = rs.getInt(5);
            int stopPos = rs.getInt(6);

            List<TranscriptFeatureCacheEntry> list = new ArrayList<>();
            if(result!= null && result.containsKey(transcriptRgdId))
                list = result.get(transcriptRgdId);
            list.add(new TranscriptFeatureCacheEntry(transcriptRgdId,objectName,strand,startPos,stopPos,chr));
            result.put(transcriptRgdId,list);
        }
        conn.close();

        return result.size();
    }

    /** return list of transcript rgd ids for geneRgdId
     *
     * @param transcriptRgdId
     * @return list of matching transcript rgd ids, possibly empty
     */
    List<TranscriptFeatureCacheEntry> getTranscriptFeatures(int transcriptRgdId) {
        if(result.containsKey(transcriptRgdId))
            return result.get(transcriptRgdId);
        else return null;
    }

    class TranscriptFeatureCacheEntry {
        public int transcriptRgdId;
        public String objectName;
        public String strand;
        public int startPos;
        public int stopPos;
        public String chromosome;

        public TranscriptFeatureCacheEntry(int transcriptRgdId, String objectName, String strand,int startPos, int stopPos, String chromosome) {
            this.transcriptRgdId = transcriptRgdId;
            this.objectName = objectName;
            this.strand = strand;
            this.startPos = startPos;
            this.stopPos = stopPos;
            this.chromosome = chromosome;
        }
    }
}
