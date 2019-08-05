package edu.mcw.rgd.ratcn;


import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 11/15/13
 * convenience class for inserting variant_transcript rows in batches
 */
public class VariantTranscriptBatch {

    public static final int BATCH_SIZE = 1000;

    // cache of records accumulated in the batch
    private List<VariantTranscript> batch = new ArrayList<>();

    private int rowsCommitted = 0;
    private int rowsUpToDate = 0;
    private boolean verifyIfInRgd = false;
    private String tableNameVT;
    private String tableNameV;
    FileWriter csvWriter;
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

    /// preload existing variant transcript data for the entire chromosome
    /// useful for ClinVar data
    public int preloadVariantTranscriptData(int sampleId, String chr) throws Exception {
        String sql = "SELECT variant_id,transcript_rgd_id,variant_transcript_id FROM "+tableNameVT+" vt \n" +
                "WHERE EXISTS(SELECT 1 FROM "+tableNameV+" v WHERE v.variant_id=vt.variant_id AND v.sample_id=? AND chromosome=?)";

        vtData = new HashMap();
csvWriter  = new FileWriter("transcript.csv");
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

  /*      BatchSqlUpdate bsu = new BatchSqlUpdate(DataSourceFactory.getInstance().getCarpeNovoDataSource(),
                "INSERT INTO "+tableNameVT+ "\n" +
                "(VARIANT_TRANSCRIPT_ID, VARIANT_ID, TRANSCRIPT_RGD_ID, REF_AA,\n" +
                "VAR_AA, SYN_STATUS, LOCATION_NAME, NEAR_SPLICE_SITE,\n" +
                "FULL_REF_AA_POS, FULL_REF_NUC_POS, TRIPLET_ERROR, FULL_REF_AA, FULL_REF_NUC, FRAMESHIFT)\n" +
                "VALUES(VARIANT_TRANSCRIPT_SEQ.nextval, ?, ?, ?,\n" +
                " ?, ?, ?, ?,\n" +
                "?,?,?,?,?,?)",
                new int[]{Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
                },10000);

        bsu.compile();
    */
        for( VariantTranscript vt: batch ) {

     csvWriter.append(Long.toString(vt.getVariantId()));
     csvWriter.append(",");
            csvWriter.append(Long.toString(vt.getTranscriptRgdId()));
            csvWriter.append(",");
            csvWriter.append(vt.getRefAA());
            csvWriter.append(",");
            csvWriter.append(vt.getVarAA());
            csvWriter.append(",");
            csvWriter.append(vt.getSynStatus());
            csvWriter.append(",");
            csvWriter.append(vt.getLocationName());
            csvWriter.append(",");
            csvWriter.append(vt.getNearSpliceSite());
            csvWriter.append(",");
            csvWriter.append(Long.toString(vt.getFullRefAAPos()));
            csvWriter.append(",");
            csvWriter.append(Long.toString(vt.getFullRefNucPos()));
            csvWriter.append(",");
            csvWriter.append(vt.getTripletError());
            csvWriter.append(",");
            csvWriter.append(vt.getFullRefAA());
            csvWriter.append(",");
            csvWriter.append(vt.getFullRefNuc());
            csvWriter.append(",");
            csvWriter.append(vt.getFrameShift());
            csvWriter.append("\n");
            /*      bsu.update(
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
            );
    */

        }

    //    bsu.flush();
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
                    rowsUpToDate++;
                    it.remove();
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
                List results = q.execute(vt.getVariantId(), vt.getTranscriptRgdId());
                if (!results.isEmpty()) {
                    rowsUpToDate++;
                    it.remove();
                }
            }
        }

        insertRowsNoVerify();
    }

    public boolean isVerifyIfInRgd() {
        return verifyIfInRgd;
    }

    public void setVerifyIfInRgd(boolean verifyIfInRgd) {
        this.verifyIfInRgd = verifyIfInRgd;
    }
}
