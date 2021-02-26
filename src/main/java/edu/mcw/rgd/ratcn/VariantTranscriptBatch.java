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
            }
            return 1;
        }
    });

    private int rowsCommitted = 0;
    private int rowsUpToDate = 0;
    private boolean verifyIfInRgd = false;


    public VariantTranscriptBatch() {
           }

    public int getRowsCommitted() {
        return rowsCommitted;
    }

    public int getRowsUpToDate() {
        return rowsUpToDate;
    }

    /// preload existing variant transcript data for the entire chromosome
    /// useful for ClinVar data
    public int preloadVariantTranscriptData(int mapKey, String chr) throws Exception {
        String sql = "SELECT variant_rgd_id,transcript_rgd_id FROM variant_transcript vt \n" +
                "WHERE EXISTS(SELECT 1 FROM variant_map_data v WHERE v.rgd_id=vt.variant_id AND vt.map_key=? AND v.chromosome=?)";

        vtData = new HashMap();
        Connection conn = DataSourceFactory.getInstance().getDataSource("Variant").getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1,mapKey);
        ps.setString(2, chr);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            // KEY(variant_id,transcript_rgd_id)
            int key = rs.getInt(1);
            // VALUE(variant_transcript_id)
            int value = rs.getInt(2);

            List<Integer> values = vtData.get(key);
            if( values==null ) {
                values = new ArrayList<>();
            }
            values.add(value);
            vtData.put(key, values);

        }
        conn.close();

        return vtData.size();
    }
    private Map<Integer, List<Integer>> vtData = null; // KEY(variant_id,transcript_rgd_id) ==> VALUE(variant_transcript_id)

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

      BatchSqlUpdate bsu = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource("Variant"),
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

    void insertRowsWithVerify() throws Exception {

        if( vtData!=null ) {
            // use preloaded data

            // remove from batch rows that are already in rgd
            Iterator<VariantTranscript> it = batch.iterator();
            while( it.hasNext() ) {
                VariantTranscript vt = it.next();
                long key = vt.getVariantId();
                //+","+vt.getTranscriptRgdId();
                List<Integer> results = vtData.get(key);
                if( results!=null ) {
                    for(int result:results){
                        if(result == vt.getTranscriptRgdId()){
                            rowsUpToDate++;
                            it.remove();
                        }
                    }

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
