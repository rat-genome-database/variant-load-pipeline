package edu.mcw.rgd.ratcn;

import org.springframework.jdbc.object.*;

import javax.sql.DataSource;
import java.sql.Types;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 8/9/13
 * Time: 11:39 AM
 * update genic status of VARIANT table in batches
 */
public class UpdateVariantStatus {

    public static final int BATCH_SIZE = 1000;

    private long[] genicVariantIds = new long[BATCH_SIZE];
    private long[] intergenicVariantIds = new long[BATCH_SIZE];

    private int genicCount = 0;
    private int intergenicCount = 0;

    public void updateGenicStatus(boolean isGenic, long variantId, DataSource ds) throws Exception {

        if( isGenic ) {
            genicVariantIds[genicCount++] = variantId;
            if( genicCount==BATCH_SIZE ) {
                commitBatch(genicVariantIds, genicCount, "GENIC", ds);
                genicCount = 0;
            }
        }
        else {
            intergenicVariantIds[intergenicCount++] = variantId;
            if( intergenicCount==BATCH_SIZE ) {
                commitBatch(intergenicVariantIds, intergenicCount, "INTERGENIC", ds);
                intergenicCount = 0;
            }
        }
    }

    public void commit(DataSource ds) throws Exception {
        commitBatch(genicVariantIds, genicCount, "GENIC", ds);
        genicCount = 0;
        commitBatch(intergenicVariantIds, intergenicCount, "INTERGENIC", ds);
        intergenicCount = 0;
    }

    void commitBatch(long[] variantIds, int count, String genicStatus, DataSource ds) throws Exception {

        if( count<=0 ) {
            return; // NOOP
        }

        String sql = "update variant set GENIC_STATUS = ? where variant_id = ?";
        BatchSqlUpdate su = new BatchSqlUpdate(ds, sql,
                new int[]{Types.VARCHAR, Types.INTEGER},
                count);
        su.compile();
        for( int i=0; i<count; i++ ) {
            su.update(genicStatus, variantIds[i]);
        }
        executeBatch(su);
    }

    public int executeBatch(BatchSqlUpdate bsu) {
        bsu.flush();

        // compute nr of rows affected
        int totalRowsAffected = 0;
        for( int rowsAffected: bsu.getRowsAffected() ) {
            totalRowsAffected += rowsAffected;
        }
        return totalRowsAffected;
    }
}
