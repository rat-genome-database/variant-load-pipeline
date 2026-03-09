package edu.mcw.rgd.ratcn;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 3/10/14
 * Time: 12:38 PM
 * <p>
 * gene rgd ids searchable by position and chromosome
 */
public class GeneCache {

    List<GeneCacheEntry> entries = new ArrayList<>();

    public int loadCache(int mapKey, String chromosome, DataSource ds) throws SQLException {

        entries.clear();

        String sql = "SELECT md.rgd_id,md.start_pos,md.stop_pos "+
	        "FROM maps_data md, rgd_ids r, genes g "+
			"WHERE md.chromosome=? AND md.map_key=? "+
			"AND md.rgd_id = r.RGD_ID and r.rgd_id = g.rgd_id "+
			"and r.OBJECT_STATUS = 'ACTIVE' and r.OBJECT_KEY = 1 "+
            "ORDER BY start_pos, stop_pos";
        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, chromosome);
        ps.setInt(2, mapKey);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            entries.add(new GeneCacheEntry(rs.getInt(1), rs.getInt(2), rs.getInt(3)));
        }
        conn.close();

        return entries.size();
    }

    /** return list of rgd ids for genes that overlap a single position (SNVs)
     *
     * @param pos variant position
     * @return list of matching gene rgd ids, possibly empty
     */
    List<Integer> getGeneRgdIds(int pos) {
        return getGeneRgdIds(pos, pos);
    }

    /** return list of rgd ids for genes that overlap the variant range [varStart, varStop]
     *
     * @param varStart variant start position
     * @param varStop variant stop position
     * @return list of matching gene rgd ids, possibly empty
     */
    List<Integer> getGeneRgdIds(int varStart, int varStop) {

        List<Integer> results = new ArrayList<>();

        // Binary search: find first entry where stopPos >= varStart
        // (any gene ending before the variant starts cannot overlap)
        // Entries are sorted by startPos, stopPos (from SQL ORDER BY)
        int lo = 0, hi = entries.size() - 1;
        int firstCandidate = entries.size();
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (entries.get(mid).stopPos >= varStart) {
                firstCandidate = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }

        // Scan forward from firstCandidate; stop when gene starts after variant ends
        for (int i = firstCandidate; i < entries.size(); i++) {
            GeneCacheEntry entry = entries.get(i);
            if (entry.startPos > varStop) break;
            // Interval overlap: variant [varStart,varStop] overlaps gene [startPos,stopPos]
            if (varStart <= entry.stopPos && varStop >= entry.startPos) {
                results.add(entry.rgdId);
            }
        }

        return results;
    }

    class GeneCacheEntry {
        public int rgdId;
        public int startPos;
        public int stopPos;

        public GeneCacheEntry(int rgdId, int startPos, int stopPos) {
            this.rgdId = rgdId;
            this.startPos = startPos;
            this.stopPos = stopPos;
        }
    }
}
