package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.datamodel.Transcript;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 3/10/14
 * Time: 12:38 PM
 * <p>
 * gene rgd ids searchable by position and chromosome
 */
public class TranscriptCache {

    List<TranscriptCacheEntry> entries = new ArrayList<>();
    HashMap<Integer,List<TranscriptCacheEntry>> result = new HashMap<Integer,List<TranscriptCacheEntry>>();
    HashMap<Integer,Integer> exonResult = new HashMap<Integer,Integer>();
    public int loadCache(int mapKey, String chromosome, DataSource ds) throws SQLException {

        entries.clear();

        String sql = "SELECT transcript_rgd_id,gene_rgd_id,is_non_coding_ind FROM transcripts WHERE \n" +
                    "EXISTS(SELECT 1 FROM maps_data md WHERE md.rgd_id=transcript_rgd_id AND md.map_key=? AND md.chromosome = ?)";
        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, mapKey);
        ps.setString(2, chromosome);
        ResultSet rs = ps.executeQuery();

        String sql1 = "SELECT t.transcript_rgd_id,count(*) FROM transcripts t inner join TRANSCRIPT_FEATURES tf on t.TRANSCRIPT_RGD_ID = tf.TRANSCRIPT_RGD_ID \n" +
                      "inner join rgd_ids r on  tf.FEATURE_RGD_ID = r.rgd_id AND r.OBJECT_KEY=15 \n" +
                      "inner join maps_data md on md.MAP_KEY=? AND md.CHROMOSOME=? AND md.rgd_id=tf.FEATURE_RGD_ID group by t.transcript_rgd_id ";

        PreparedStatement ps1 = conn.prepareStatement(sql1);

        ps1.setInt(1, mapKey);
        ps1.setString(2, chromosome);
        ResultSet rs1 = ps1.executeQuery();
        while( rs1.next() ) {
            int transcriptRgdId = rs1.getInt(1);
            int exonCount = rs1.getInt(2);
            exonResult.put(transcriptRgdId,exonCount);
        }


        while( rs.next() ) {
            int transcriptRgdId = rs.getInt(1);
            int geneRgdId = rs.getInt(2);
            String isNonCoding = rs.getString(3);
            List<TranscriptCacheEntry> list = new ArrayList<>();
            entries.add(new TranscriptCacheEntry(transcriptRgdId,geneRgdId,isNonCoding));
            if(result!= null && result.containsKey(geneRgdId))
                list = result.get(geneRgdId);
            list.add(new TranscriptCacheEntry(transcriptRgdId,geneRgdId,isNonCoding));
            result.put(geneRgdId,list);
        }
        conn.close();

        return entries.size();
    }

    /** return list of transcript rgd ids for geneRgdId
     *
     * @param geneRgdId
     * @return list of matching transcript rgd ids, possibly empty
     */
    List<TranscriptCacheEntry> getTranscripts(int geneRgdId) {
        if(result.containsKey(geneRgdId))
            return result.get(geneRgdId);
        else return null;
    }

    class TranscriptCacheEntry {
        public int transcriptRgdId;
        public int geneRgdId;
        public String isNonCodingRegion;

        public TranscriptCacheEntry(int transcriptRgdId, int geneRgdId, String isNonCodingRegion) {
            this.transcriptRgdId = transcriptRgdId;
            this.geneRgdId = geneRgdId;
            this.isNonCodingRegion = isNonCodingRegion;

        }
    }
}
