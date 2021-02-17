package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameter;

import java.sql.*;
import java.util.*;

/**
 * @author mtutaj
 * @since 11/15/13
 * convenience class for inserting variant_transcript rows in batches
 */
public class VariantTranscriptBatch {

    private Logger logDebug = Logger.getLogger("debug");

    public static final int BATCH_SIZE = 500;

    // cache of records accumulated in the batch
    private List<VariantTranscript> batch = new ArrayList<>();

    private int rowsCommitted = 0;
    private int rowsUpToDate = 0;
    private int rowsUpdated = 0;
    private boolean verifyIfInRgd = false;
    private String tableNameVT;
    private String tableNameV;

    public VariantTranscriptBatch(int sampleId) {
        VariantDAO vdao = new VariantDAO();
        tableNameVT = vdao.getVariantTranscriptTable(sampleId);
        tableNameV = vdao.getVariantTable(sampleId);
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
    public int preloadVariantTranscriptData(int sampleId, String chr) throws Exception {
        String sql = "SELECT variant_id,transcript_rgd_id,variant_transcript_id FROM "+tableNameVT+" vt \n" +
                "WHERE EXISTS(SELECT 1 FROM "+tableNameV+" v WHERE v.variant_id=vt.variant_id AND v.sample_id=? AND chromosome=?)";

        vtData = new HashMap();

        Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, sampleId);
        ps.setString(2, chr);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            // KEY(variant_id,transcript_rgd_id)
            String key = rs.getLong(1)+","+rs.getLong(2);
            // VALUE(variant_transcript_id)
            Long value = rs.getLong(3);

            List<Long> values = vtData.get(key);
            if( values==null ) {
                values = new ArrayList<>();
                vtData.put(key, values);
            }
            values.add(value);
        }
        conn.close();

        return vtData.size();
    }
    private Map<String, List<Long>> vtData = null; // KEY(variant_id,transcript_rgd_id) ==> VALUE(variant_transcript_id)

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
        //System.out.println("  VARIANT_TRANSCRIPT inserted rows "+rowsCommitted);
        return affectedRows;
    }

    void insertRowsNoVerify() throws Exception {
        if( batch.isEmpty() )
            return;

        List batchArgs = new ArrayList(batch.size());
        for( VariantTranscript vt: batch ) {
            batchArgs.add(new Object[]{
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
                vt.getFullRefAA(),
                vt.getFullRefNuc(),
                vt.getFrameShift()
            });
        }
        String sql = "INSERT INTO "+tableNameVT+" \n"+
                "(VARIANT_TRANSCRIPT_ID, VARIANT_ID, TRANSCRIPT_RGD_ID, REF_AA, VAR_AA, SYN_STATUS, LOCATION_NAME, NEAR_SPLICE_SITE, "+
                "FULL_REF_AA_POS, FULL_REF_NUC_POS, TRIPLET_ERROR, FULL_REF_AA, FULL_REF_NUC, FRAMESHIFT) "+
                "VALUES(VARIANT_TRANSCRIPT_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, "+
                "?,?,?,?,?,?)";
        new JdbcTemplate(DataSourceFactory.getInstance().getCarpeNovoDataSource())
            .batchUpdate(sql, batchArgs);
    }

    void insertRowsWithVerify() throws Exception {

        if( vtData!=null ) {
            // use preloaded data

            // remove from batch rows that are already in rgd
            Iterator<VariantTranscript> it = batch.iterator();
            while( it.hasNext() ) {
                VariantTranscript vt = it.next();
                String key = vt.getVariantId()+","+vt.getTranscriptRgdId();
                List results = vtData.get(key);
                if( results!=null ) {
                    it.remove();
                    Long vtid = (Long) results.get(0);
                    if( updateVariantTranscript(vt, vtid) ) {
                        rowsUpdated++;
                    } else {
                        rowsUpToDate++;
                    }
                }
            }
        } else {

            String sql = "SELECT variant_transcript_id FROM " + tableNameVT + " WHERE variant_id=? AND transcript_rgd_id=?";
            StringListQuery q = new StringListQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
            q.declareParameter(new SqlParameter(Types.INTEGER));
            q.declareParameter(new SqlParameter(Types.INTEGER));
            q.compile();

            // remove from batch rows that are already in rgd
            Iterator<VariantTranscript> it = batch.iterator();
            while (it.hasNext()) {
                VariantTranscript vt = it.next();
                List<String> results = q.execute(vt.getVariantId(), vt.getTranscriptRgdId());
                if (!results.isEmpty()) {
                    long vtid = Long.parseLong(results.get(0));
                    it.remove();
                    if( updateVariantTranscript(vt, vtid) ) {
                        rowsUpdated++;
                    } else {
                        rowsUpToDate++;
                    }
                }
            }
        }

        insertRowsNoVerify();
    }

    boolean updateVariantTranscript(VariantTranscript vt, long vtid) throws Exception {

        //logDebug.debug("  vtid="+vtid);

        boolean updateNeeded = false;

        try( Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection() ) {
            String sql = "SELECT * FROM " + tableNameVT + " WHERE variant_transcript_id=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setLong(1, vtid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {

                // check if update need
                String refAa = rs.getString("ref_aa");
                boolean refAaOK = Utils.stringsAreEqual(vt.getRefAA(), refAa);
                String varAa = rs.getString("var_aa");
                boolean varAaOK = Utils.stringsAreEqual(vt.getVarAA(), varAa);
                String geneSplice = rs.getString("genesplice_status");
                boolean geneSpliceOK = Utils.stringsAreEqual(vt.getGenespliceStatus(), geneSplice);
                String synStatus = rs.getString("syn_status");
                boolean synStatusOK = Utils.stringsAreEqual(vt.getSynStatus(), synStatus);
                String locationName = rs.getString("location_name");
                boolean locationNameOK = Utils.stringsAreEqual(vt.getLocationName(), locationName);
                String nearSpliceSite = rs.getString("near_splice_site");
                boolean nearSpliceSiteOK = Utils.stringsAreEqual(vt.getNearSpliceSite(), nearSpliceSite);
                String fullRefNuc = rs.getString("full_ref_nuc");
                boolean fullRefNucOK = Utils.stringsAreEqual(vt.getFullRefNuc(), fullRefNuc);
                String fullRefAa = rs.getString("full_ref_aa");
                boolean fullRefAaOK = Utils.stringsAreEqual(vt.getFullRefAA(), fullRefAa);
                int fullRefNucPos = rs.getInt("full_ref_nuc_pos");
                boolean fullRefNucPosOK = Utils.intsAreEqual(vt.getFullRefNucPos(), fullRefNucPos);
                int fullRefAaPos = rs.getInt("full_ref_aa_pos");
                boolean fullRefAaPosOK = Utils.intsAreEqual(vt.getFullRefAAPos(), fullRefAaPos);

                String uniprotId = rs.getString("uniprot_id");
                boolean uniprotIdOK = Utils.stringsAreEqual(vt.getUniprotId(), uniprotId);
                String proteinId = rs.getString("protein_id");
                boolean proteinIdOK = Utils.stringsAreEqual(vt.getProteinId(), proteinId);
                String tripletError = rs.getString("triplet_error");
                boolean tripletErrorOK = Utils.stringsAreEqual(vt.getTripletError(), tripletError);
                String frameshift = rs.getString("frameshift");
                boolean frameshiftOK = Utils.stringsAreEqual(vt.getFrameShift(), frameshift);

                if (refAaOK && varAaOK && geneSpliceOK && synStatusOK && locationNameOK && nearSpliceSiteOK
                        && fullRefNucOK && fullRefAaOK && fullRefNucPosOK && fullRefAaPosOK
                        && uniprotIdOK && proteinIdOK && tripletErrorOK && frameshiftOK) {

                    updateNeeded = false; // incoming data the same as in RGD
                } else {
                    updateNeeded = true;
                }
            }
            rs.close();
            ps.close();

            if (updateNeeded) {

                String sql2 = "UPDATE " + tableNameVT + " SET " +
                        "ref_aa=?, var_aa=?, genesplice_status=?, syn_status=?, location_name=?, near_splice_site=?, " +
                        "full_ref_nuc=?, full_ref_aa=?, full_ref_nuc_pos=?, full_ref_aa_pos=?, " +
                        "uniprot_id=?, protein_id=?, triplet_error=?, frameshift=? " +
                        " WHERE variant_transcript_id=?";

                PreparedStatement ps2 = conn.prepareStatement(sql2);
                ps2.setString(1, vt.getRefAA());
                ps2.setString(2, vt.getVarAA());
                ps2.setString(3, vt.getGenespliceStatus());
                ps2.setString(4, vt.getSynStatus());
                ps2.setString(5, vt.getLocationName());
                ps2.setString(6, vt.getNearSpliceSite());

                ps2.setString(7, vt.getFullRefNuc());
                ps2.setString(8, vt.getFullRefAA());
                if( vt.getFullRefNucPos()==null ) {
                    ps2.setNull(9, Types.INTEGER);
                } else {
                    ps2.setInt(9, vt.getFullRefNucPos());
                }
                if( vt.getFullRefAAPos()==null ) {
                    ps2.setNull(10, Types.INTEGER);
                } else {
                    ps2.setInt(10, vt.getFullRefAAPos());
                }

                ps2.setString(11, vt.getUniprotId());
                ps2.setString(12, vt.getProteinId());
                ps2.setString(13, vt.getTripletError());
                ps2.setString(14, vt.getFrameShift());

                ps2.setLong(15, vtid);

                ps2.executeUpdate();
                ps2.close();
            }
        }
        return updateNeeded;
    }

    public boolean isVerifyIfInRgd() {
        return verifyIfInRgd;
    }

    public void setVerifyIfInRgd(boolean verifyIfInRgd) {
        this.verifyIfInRgd = verifyIfInRgd;
    }
}
