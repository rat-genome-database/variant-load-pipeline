package edu.mcw.rgd.ratcn;


import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;


/**
 * @author mtutaj
 * @since 11/15/13
 * convenience class for inserting variant_transcript rows in batches
 * MODIFIED: Now supports updating existing records when values differ
 */
public class VariantTranscriptBatch {

    public static final int BATCH_SIZE = 5000;

    // cache of records accumulated in the batch
    private Set<VariantTranscript> batch = new TreeSet<>(new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
            VariantTranscript vt1 = (VariantTranscript) o1;
            VariantTranscript vt2 = (VariantTranscript) o2;
            if(vt1.getVariantId() == vt2.getVariantId()) {
                if(vt1.getTranscriptRgdId() == vt2.getTranscriptRgdId())
                    return 0;
                else{
                    if(vt1.getTranscriptRgdId() < vt2.getTranscriptRgdId())
                        return 1;
                    else return -1;
                }
            }else {
                if(vt1.getVariantId() < vt2.getVariantId())
                    return 1;
                else return -1;
            }

        }
    });

    private int rowsCommitted = 0;
    private int rowsUpToDate = 0;
    private int rowsUpdated = 0;  // NEW: track updated records
    private boolean verifyIfInRgd = true;


    public VariantTranscriptBatch() {
           }

    public int getRowsCommitted() {
        return rowsCommitted;
    }

    public int getRowsUpToDate() {
        return rowsUpToDate;
    }

    public int getRowsUpdated() {
        return rowsUpdated;
    }

    /// preload existing variant transcript data for the entire chromosome
    /// useful for ClinVar data
    /// MODIFIED: Now loads complete records with all fields for comparison
    public int preloadVariantTranscriptData(int mapKey, String chr) throws Exception {
        String sql = "SELECT variant_rgd_id, transcript_rgd_id, ref_aa, var_aa, syn_status, " +
                "location_name, near_splice_site, full_ref_aa_pos, full_ref_nuc_pos, " +
                "triplet_error, full_ref_aa_seq_key, full_ref_nuc_seq_key, frameshift " +
                "FROM variant_transcript vt \n" +
                "WHERE EXISTS(SELECT 1 FROM variant_map_data v WHERE v.rgd_id=vt.variant_rgd_id AND v.map_key=? AND v.chromosome=?) " +
                "AND vt.map_key=?";

        vtData = new HashMap<>();
        Connection conn = DataSourceFactory.getInstance().getDataSource("Carpe").getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, mapKey);
        ps.setString(2, chr);
        ps.setInt(3, mapKey);
        ResultSet rs = ps.executeQuery();

        while( rs.next() ) {
            VariantTranscript vt = new VariantTranscript();
            vt.setVariantId(rs.getLong(1));
            vt.setTranscriptRgdId(rs.getInt(2));
            vt.setRefAA(rs.getString(3));
            vt.setVarAA(rs.getString(4));
            vt.setSynStatus(rs.getString(5));
            vt.setLocationName(rs.getString(6));
            vt.setNearSpliceSite(rs.getString(7));

            // Handle nullable integers
            int aaPos = rs.getInt(8);
            vt.setFullRefAAPos(rs.wasNull() ? null : aaPos);

            int nucPos = rs.getInt(9);
            vt.setFullRefNucPos(rs.wasNull() ? null : nucPos);

            vt.setTripletError(rs.getString(10));

            int aaSeqKey = rs.getInt(11);
            vt.setFullRefAASeqKey(rs.wasNull() ? 0 : aaSeqKey);

            int nucSeqKey = rs.getInt(12);
            vt.setFullRefNucSeqKey(rs.wasNull() ? 0 : nucSeqKey);

            vt.setFrameShift(rs.getString(13));
            vt.setMapKey(mapKey);

            // Create composite key: variant_rgd_id + "_" + transcript_rgd_id
            String key = vt.getVariantId() + "_" + vt.getTranscriptRgdId();
            vtData.put(key, vt);
        }
        conn.close();

        return vtData.size();
    }

    // MODIFIED: Now stores complete VariantTranscript objects
    private Map<String, VariantTranscript> vtData = null;

    // Map key for selective preload - set when adding to batch
    private int currentMapKey = 0;

    /**
     * Preload existing variant transcript data for only the variants in the current batch.
     * More efficient than loading entire chromosome when processing a subset of variants.
     * Automatically handles Oracle's 1000-item IN clause limit.
     */
    private void preloadForBatch() throws Exception {
        if (batch.isEmpty()) {
            vtData = new HashMap<>();
            return;
        }

        // Collect unique variant IDs from the batch
        Set<Long> variantIds = new HashSet<>();
        for (VariantTranscript vt : batch) {
            variantIds.add(vt.getVariantId());
            if (currentMapKey == 0) {
                currentMapKey = vt.getMapKey();
            }
        }

        vtData = new HashMap<>();
        List<Long> idList = new ArrayList<>(variantIds);

        // Oracle has a limit of 1000 items in IN clause - chunk if needed
        int chunkSize = 1000;

        Connection conn = DataSourceFactory.getInstance().getDataSource("Carpe").getConnection();

        for (int i = 0; i < idList.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, idList.size());
            List<Long> chunk = idList.subList(i, end);

            // Build placeholders for IN clause
            StringBuilder placeholders = new StringBuilder();
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) placeholders.append(",");
                placeholders.append("?");
            }

            String sql = "SELECT variant_rgd_id, transcript_rgd_id, ref_aa, var_aa, syn_status, " +
                    "location_name, near_splice_site, full_ref_aa_pos, full_ref_nuc_pos, " +
                    "triplet_error, full_ref_aa_seq_key, full_ref_nuc_seq_key, frameshift " +
                    "FROM variant_transcript " +
                    "WHERE variant_rgd_id IN (" + placeholders + ") AND map_key=?";

            PreparedStatement ps = conn.prepareStatement(sql);
            int paramIndex = 1;
            for (Long variantId : chunk) {
                ps.setLong(paramIndex++, variantId);
            }
            ps.setInt(paramIndex, currentMapKey);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                VariantTranscript vt = new VariantTranscript();
                vt.setVariantId(rs.getLong(1));
                vt.setTranscriptRgdId(rs.getInt(2));
                vt.setRefAA(rs.getString(3));
                vt.setVarAA(rs.getString(4));
                vt.setSynStatus(rs.getString(5));
                vt.setLocationName(rs.getString(6));
                vt.setNearSpliceSite(rs.getString(7));

                int aaPos = rs.getInt(8);
                vt.setFullRefAAPos(rs.wasNull() ? null : aaPos);

                int nucPos = rs.getInt(9);
                vt.setFullRefNucPos(rs.wasNull() ? null : nucPos);

                vt.setTripletError(rs.getString(10));

                int aaSeqKey = rs.getInt(11);
                vt.setFullRefAASeqKey(rs.wasNull() ? 0 : aaSeqKey);

                int nucSeqKey = rs.getInt(12);
                vt.setFullRefNucSeqKey(rs.wasNull() ? 0 : nucSeqKey);

                vt.setFrameShift(rs.getString(13));
                vt.setMapKey(currentMapKey);

                String key = vt.getVariantId() + "_" + vt.getTranscriptRgdId();
                vtData.put(key, vt);
            }

            rs.close();
            ps.close();
        }

        conn.close();
    }

    /**
     * Check if two VariantTranscript objects have different field values
     * @param existing The existing record from database
     * @param newVt The newly calculated record
     * @return true if any field differs
     */
    private boolean recordsDiffer(VariantTranscript existing, VariantTranscript newVt) {
        // Compare all relevant fields
        if (!stringsEqual(existing.getRefAA(), newVt.getRefAA())) return true;
        if (!stringsEqual(existing.getVarAA(), newVt.getVarAA())) return true;
        if (!stringsEqual(existing.getSynStatus(), newVt.getSynStatus())) return true;
        if (!stringsEqual(existing.getLocationName(), newVt.getLocationName())) return true;
        if (!stringsEqual(existing.getNearSpliceSite(), newVt.getNearSpliceSite())) return true;
        if (!stringsEqual(existing.getTripletError(), newVt.getTripletError())) return true;
        if (!stringsEqual(existing.getFrameShift(), newVt.getFrameShift())) return true;

        // Compare nullable integers
        if (!integersEqual(existing.getFullRefAAPos(), newVt.getFullRefAAPos())) return true;
        if (!integersEqual(existing.getFullRefNucPos(), newVt.getFullRefNucPos())) return true;

        // Compare sequence keys (0 means null in this context)
        if (existing.getFullRefAASeqKey() != newVt.getFullRefAASeqKey()) return true;
        if (existing.getFullRefNucSeqKey() != newVt.getFullRefNucSeqKey()) return true;

        return false;
    }

    private boolean stringsEqual(String s1, String s2) {
        if (s1 == null && s2 == null) return true;
        if (s1 == null || s2 == null) return false;
        return s1.equals(s2);
    }

    private boolean integersEqual(Integer i1, Integer i2) {
        if (i1 == null && i2 == null) return true;
        if (i1 == null || i2 == null) return false;
        return i1.equals(i2);
    }

    /**
     *
     * @param vt VariantTranscript object
     * @return count of rows written to database
     */
    public int addToBatch(VariantTranscript vt) throws Exception {
        batch.add(vt);
        if( batch.size()>=BATCH_SIZE )
            return flush();
        else
            return 0;
    }

    /**
     *
     * @return count of rows written to database
     */
    public int flush() throws Exception {
        if( batch.isEmpty() )
            return 0;

        if( isVerifyIfInRgd() )
            insertRowsWithVerify();
        else
            insertRowsNoVerify();

        int affectedRows = batch.size();
        rowsCommitted += affectedRows;
        batch.clear();
        return affectedRows;
    }

    void insertRowsNoVerify() throws Exception {
        if( batch.isEmpty() )
            return;

      BatchSqlUpdate bsu = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource("Carpe"),
                "INSERT INTO VARIANT_TRANSCRIPT \n" +
                "( VARIANT_RGD_ID, TRANSCRIPT_RGD_ID, REF_AA,\n" +
                "VAR_AA, SYN_STATUS, LOCATION_NAME, NEAR_SPLICE_SITE,\n" +
                "FULL_REF_AA_POS, FULL_REF_NUC_POS, TRIPLET_ERROR, FULL_REF_AA_SEQ_KEY, FULL_REF_NUC_SEQ_KEY, FRAMESHIFT,MAP_KEY)\n" +
                "VALUES( ?, ?, ?,\n" +
                " ?, ?, ?, ?,\n" +
                "?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,Types.INTEGER
                },10000);

        bsu.compile();

        for( VariantTranscript vt: batch ) {

           bsu.update(
                vt.getVariantId(),
                vt.getTranscriptRgdId(),
                vt.getRefAA(),
                vt.getVarAA(),
                vt.getSynStatus(),
                vt.getLocationName(),
                vt.getNearSpliceSite(),
                vt.getFullRefAAPos(),
                vt.getFullRefNucPos(),
                vt.getTripletError(),
                vt.getFullRefAASeqKey(),
                vt.getFullRefNucSeqKey(),
                vt.getFrameShift(),
                vt.getMapKey()
            );


        }

       bsu.flush();
    }

    /**
     * NEW METHOD: Batch update existing records
     */
    void updateRowsBatch(List<VariantTranscript> updateList) throws Exception {
        if (updateList.isEmpty())
            return;

        BatchSqlUpdate bsu = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource("Carpe"),
                "UPDATE VARIANT_TRANSCRIPT SET " +
                "REF_AA=?, VAR_AA=?, SYN_STATUS=?, LOCATION_NAME=?, NEAR_SPLICE_SITE=?, " +
                "FULL_REF_AA_POS=?, FULL_REF_NUC_POS=?, TRIPLET_ERROR=?, " +
                "FULL_REF_AA_SEQ_KEY=?, FULL_REF_NUC_SEQ_KEY=?, FRAMESHIFT=? " +
                "WHERE VARIANT_RGD_ID=? AND TRANSCRIPT_RGD_ID=? AND MAP_KEY=?",
                new int[]{
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,  // REF_AA through NEAR_SPLICE_SITE
                    Types.INTEGER, Types.INTEGER, Types.VARCHAR,                                  // AA_POS, NUC_POS, TRIPLET_ERROR
                    Types.INTEGER, Types.INTEGER, Types.VARCHAR,                                  // AA_SEQ_KEY, NUC_SEQ_KEY, FRAMESHIFT
                    Types.INTEGER, Types.INTEGER, Types.INTEGER                                   // WHERE: VARIANT_RGD_ID, TRANSCRIPT_RGD_ID, MAP_KEY
                }, 10000);

        bsu.compile();

        for (VariantTranscript vt : updateList) {
            bsu.update(
                vt.getRefAA(),
                vt.getVarAA(),
                vt.getSynStatus(),
                vt.getLocationName(),
                vt.getNearSpliceSite(),
                vt.getFullRefAAPos(),
                vt.getFullRefNucPos(),
                vt.getTripletError(),
                vt.getFullRefAASeqKey() == 0 ? null : vt.getFullRefAASeqKey(),
                vt.getFullRefNucSeqKey() == 0 ? null : vt.getFullRefNucSeqKey(),
                vt.getFrameShift(),
                vt.getVariantId(),
                vt.getTranscriptRgdId(),
                vt.getMapKey()
            );
        }

        bsu.flush();
        rowsUpdated += updateList.size();
    }

    /**
     * MODIFIED: Now compares existing records and updates if different.
     * Uses selective preload (batch-only) when chromosome-wide preload was not called.
     */
    void insertRowsWithVerify() throws Exception {

        // If no chromosome-wide preload was done, do selective preload for just this batch
        boolean useSelectivePreload = (vtData == null);
        if (useSelectivePreload) {
            preloadForBatch();
        }

        List<VariantTranscript> toUpdate = new ArrayList<>();
        List<VariantTranscript> toInsert = new ArrayList<>();

        // Categorize each record: update, insert, or skip
        for (VariantTranscript newVt : batch) {
            String key = newVt.getVariantId() + "_" + newVt.getTranscriptRgdId();
            VariantTranscript existing = vtData.get(key);

            if (existing != null) {
                // Record exists - check if values differ
                if (recordsDiffer(existing, newVt)) {
                    toUpdate.add(newVt);
                } else {
                    rowsUpToDate++;  // Truly up-to-date
                }
            } else {
                // Record doesn't exist - needs insert
                toInsert.add(newVt);
            }
        }

        // Perform batch updates
        if (!toUpdate.isEmpty()) {
            updateRowsBatch(toUpdate);
        }

        // Perform batch inserts
        if (!toInsert.isEmpty()) {
            batch.clear();
            batch.addAll(toInsert);
            insertRowsNoVerify();
        } else {
            // Nothing to insert
            batch.clear();
        }

        // Clear selective preload data after each batch to free memory
        // (chromosome-wide preload is kept for subsequent batches)
        if (useSelectivePreload) {
            vtData = null;
        }
    }

    public boolean isVerifyIfInRgd() {
        return verifyIfInRgd;
    }

    public void setVerifyIfInRgd(boolean verifyIfInRgd) {
        this.verifyIfInRgd = verifyIfInRgd;
    }
}
