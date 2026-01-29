package edu.mcw.rgd.ratcn;


import edu.mcw.rgd.dao.DataSourceFactory;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.sql.Types;
import java.util.*;


/**
 * @author mtutaj
 * @since 11/15/13
 * convenience class for inserting variant_transcript rows in batches
 * MODIFIED: Now uses Oracle MERGE for efficient insert-or-update without preloading
 */
public class VariantTranscriptBatch {

    public static final int BATCH_SIZE = 10000;

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
    private int rowsUpToDate = 0;  // Not tracked with MERGE - kept for API compatibility
    private int rowsUpdated = 0;   // Not tracked with MERGE - kept for API compatibility
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

    /**
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
     * @return count of rows written to database
     */
    public int flush() throws Exception {
        if( batch.isEmpty() )
            return 0;

        if( isVerifyIfInRgd() )
            mergeRows();
        else
            insertRowsNoVerify();

        int affectedRows = batch.size();
        rowsCommitted += affectedRows;
        batch.clear();
        return affectedRows;
    }

    /**
     * Insert rows without checking if they exist - used when verifyIfInRgd is false
     */
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
     * Use Oracle MERGE to insert new records or update existing ones in a single operation.
     * This eliminates the need for preloading and comparing records in Java.
     */
    void mergeRows() throws Exception {
        if (batch.isEmpty())
            return;

        String mergeSql =
            "MERGE INTO VARIANT_TRANSCRIPT target " +
            "USING (SELECT ? AS var_id, ? AS tr_id, ? AS ref_aa, ? AS var_aa, ? AS syn_status, " +
            "? AS location_name, ? AS near_splice_site, ? AS aa_pos, ? AS nuc_pos, " +
            "? AS triplet_error, ? AS aa_seq_key, ? AS nuc_seq_key, ? AS frameshift, ? AS map_key " +
            "FROM dual) source " +
            "ON (target.VARIANT_RGD_ID = source.var_id " +
            "AND target.TRANSCRIPT_RGD_ID = source.tr_id " +
            "AND target.MAP_KEY = source.map_key) " +
            "WHEN MATCHED THEN UPDATE SET " +
            "REF_AA = source.ref_aa, VAR_AA = source.var_aa, SYN_STATUS = source.syn_status, " +
            "LOCATION_NAME = source.location_name, NEAR_SPLICE_SITE = source.near_splice_site, " +
            "FULL_REF_AA_POS = source.aa_pos, FULL_REF_NUC_POS = source.nuc_pos, " +
            "TRIPLET_ERROR = source.triplet_error, FULL_REF_AA_SEQ_KEY = source.aa_seq_key, " +
            "FULL_REF_NUC_SEQ_KEY = source.nuc_seq_key, FRAMESHIFT = source.frameshift " +
            "WHEN NOT MATCHED THEN INSERT " +
            "(VARIANT_RGD_ID, TRANSCRIPT_RGD_ID, REF_AA, VAR_AA, SYN_STATUS, LOCATION_NAME, " +
            "NEAR_SPLICE_SITE, FULL_REF_AA_POS, FULL_REF_NUC_POS, TRIPLET_ERROR, " +
            "FULL_REF_AA_SEQ_KEY, FULL_REF_NUC_SEQ_KEY, FRAMESHIFT, MAP_KEY) " +
            "VALUES (source.var_id, source.tr_id, source.ref_aa, source.var_aa, source.syn_status, " +
            "source.location_name, source.near_splice_site, source.aa_pos, source.nuc_pos, " +
            "source.triplet_error, source.aa_seq_key, source.nuc_seq_key, source.frameshift, source.map_key)";

        BatchSqlUpdate bsu = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource("Carpe"),
                mergeSql,
                new int[]{
                    Types.INTEGER,  // var_id
                    Types.INTEGER,  // tr_id
                    Types.VARCHAR,  // ref_aa
                    Types.VARCHAR,  // var_aa
                    Types.VARCHAR,  // syn_status
                    Types.VARCHAR,  // location_name
                    Types.VARCHAR,  // near_splice_site
                    Types.INTEGER,  // aa_pos
                    Types.INTEGER,  // nuc_pos
                    Types.VARCHAR,  // triplet_error
                    Types.INTEGER,  // aa_seq_key
                    Types.INTEGER,  // nuc_seq_key
                    Types.VARCHAR,  // frameshift
                    Types.INTEGER   // map_key
                }, 10000);

        bsu.compile();

        for (VariantTranscript vt : batch) {
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
                vt.getFullRefAASeqKey() == 0 ? null : vt.getFullRefAASeqKey(),
                vt.getFullRefNucSeqKey() == 0 ? null : vt.getFullRefNucSeqKey(),
                vt.getFrameShift(),
                vt.getMapKey()
            );
        }

        bsu.flush();
    }

    public boolean isVerifyIfInRgd() {
        return verifyIfInRgd;
    }

    public void setVerifyIfInRgd(boolean verifyIfInRgd) {
        this.verifyIfInRgd = verifyIfInRgd;
    }
}
