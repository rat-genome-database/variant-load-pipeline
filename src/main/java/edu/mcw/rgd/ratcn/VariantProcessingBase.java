package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.spring.StringListQuery;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 11/11/13
 * Time: 11:47 AM
 * <p>
 * contains code reused by different variant loaders
 */
public class VariantProcessingBase {

    private DataSource dataSource;
    private BufferedWriter logWriter;
    private Logger logStatus = Logger.getLogger("status");

    public VariantProcessingBase() throws Exception {

        setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
        if( logWriter!=null )
            logWriter.write(getConnectionInfo()+"\n");
        else
            System.out.println(getConnectionInfo());
    }

    /**
     * return db user name and url
     * @return db user name and url
     */
    public String getConnectionInfo() {
        try {
            DatabaseMetaData meta = getDataSource().getConnection().getMetaData();
            return "DB user="+meta.getUserName()+", url="+meta.getURL();
        }
        catch( Exception e ) {
            e.printStackTrace();
            return "";
        }
    }

    void insertSystemLogMessage(String process, String msg) {

        String sql = "insert into SYSTEM_LOG "+
            "(SYSTEM_LOG_ID, SYSTEM_COMPONENT, LOG_LEVEL, EVENT_DATE, STRING_VALUE, FLOAT_VALUE) "+
            "values (SYSTEM_LOG_SEQ.NEXTVAL, ?,  'Info', SYSDATE, ?, 0) ";

        SqlUpdate su = new SqlUpdate(this.getDataSource(), sql);
        su.declareParameter(new SqlParameter(Types.VARCHAR));
        su.declareParameter(new SqlParameter(Types.VARCHAR));
        su.compile();
        su.update(new Object[]{msg, process});
    }

    List<String> getIndexesForTable(String tableName, String indexStatus) throws Exception {

        String sql = "SELECT index_name FROM user_indexes WHERE table_name=? AND status=? AND UNIQUENESS<>'UNIQUE'";
        StringListQuery query = new StringListQuery(this.getDataSource(), sql);
        query.declareParameter(new SqlParameter(Types.VARCHAR));
        query.declareParameter(new SqlParameter(Types.VARCHAR));
        return query.execute(tableName, indexStatus);
    }

    int disableIndexesForTable(String tableName) throws Exception {

        int count = 0;
        for( String indexName: getIndexesForTable(tableName, "VALID"))  {
            String sql = "ALTER INDEX "+indexName+" UNUSABLE";
            System.out.println(sql);

            SqlUpdate su = new SqlUpdate(this.getDataSource(), sql);
            su.compile();
            su.update();

            count++;
        }
        return count;
    }

    int enableIndexesForTable(String tableName) throws Exception {

        int count = 0;
        for( String indexName: getIndexesForTable(tableName, "UNUSABLE"))  {
            String sql = "ALTER INDEX "+indexName+" REBUILD PARALLEL NOCOMPRESS NOLOGGING";
            System.out.println(sql);

            SqlUpdate su = new SqlUpdate(this.getDataSource(), sql);
            su.compile();
            su.update();

            count++;
        }
        return count;
    }

    List<String> getConstraintsForTable(String tableName, String constraintType) throws Exception {

        String sql = "SELECT constraint_name FROM user_constraints WHERE table_name=? AND constraint_type=?";
        StringListQuery query = new StringListQuery(this.getDataSource(), sql);
        query.declareParameter(new SqlParameter(Types.VARCHAR));
        query.declareParameter(new SqlParameter(Types.VARCHAR));
        return query.execute(tableName, constraintType);
    }

    int disableConstraintsForTable(String tableName) throws Exception {

        int count = 0;
        for( String constraintName: getConstraintsForTable(tableName, "R"))  {
            String sql = "ALTER TABLE "+tableName+" DISABLE CONSTRAINT "+constraintName;
            System.out.println(sql);

            SqlUpdate su = new SqlUpdate(this.getDataSource(), sql);
            su.compile();
            su.update();

            count++;
        }
        return count;
    }

    int enableConstraintsForTable(String tableName) throws Exception {

        int count = 0;
        for( String constraintName: getConstraintsForTable(tableName, "R"))  {
            String sql = "ALTER TABLE "+tableName+" ENABLE CONSTRAINT "+constraintName;
            System.out.println(sql);

            SqlUpdate su = new SqlUpdate(this.getDataSource(), sql);
            su.compile();
            su.update();

            count++;
        }
        return count;
    }


    public List<File> listFilesRecursively(File dir, String ext1, String ext2) {

        List<File> results = new ArrayList<File>();
        listFilesRecursively(dir, ext1, ext2, results);
        return results;
    }

    public void listFilesRecursively(File dir, String ext1, String ext2, List<File> results) {

        File[] files = dir.listFiles();
        if( files==null )
            return;

        for( File file: files ) {
            if( file.isDirectory() ) {
                listFilesRecursively(file, ext1, ext2, results);
                continue;
            }

            if( ext1==null && ext2==null ) {
                // no extension filter
                results.add(file);
                continue;
            }

            String fileName = file.getName();
            if( ext1!=null && fileName.endsWith(ext1) ) {
                // apply 1st extension filter
                results.add(file);
                continue;
            }
            if( ext2!=null && fileName.endsWith(ext2) ) {
                // apply 2nd extension filter
                results.add(file);
            }
        }
    }
    public List<VariantMapData> getVariants(int speciesKey, int mapKey,String chr) throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vm on v.rgd_id = vm. rgd_id WHERE v.species_type_key = ? and vm.map_key = ? and vm.chromosome=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(speciesKey,mapKey,chr);
    }
    public void insertVariants(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql1 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant (\n" +
                        " RGD_ID,REF_NUC, VARIANT_TYPE, VAR_NUC, RS_ID, CLINVAR_ID, SPECIES_TYPE_KEY)\n" +
                        "VALUES (\n" +
                        "  ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.INTEGER}, 10000);
        sql1.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql1.update(id, v.getReferenceNucleotide(), v.getVariantType(), v.getVariantNucleotide(), v.getRsId(), v.getClinvarId(), v.getSpeciesTypeKey());

        }
        sql1.flush();
    }
    public void insertVariantMapData(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_map_data (\n" +
                        " RGD_ID,CHROMOSOME,START_POS,END_POS,PADDING_BASE,GENIC_STATUS,MAP_KEY)\n" +
                        "VALUES (\n" +
                        " ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,Types.VARCHAR, Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(id, v.getChromosome(), v.getStartPos(), v.getEndPos(), v.getPaddingBase(), v.getGenicStatus(), v.getMapKey());
        }
        sql2.flush();
    }
    public int insertVariantSample(List<VariantSampleDetail> sampleData) throws Exception {
        BatchSqlUpdate bsu= new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_sample_detail (\n" +
                        " RGD_ID,SOURCE,SAMPLE_ID,TOTAL_DEPTH,VAR_FREQ,ZYGOSITY_STATUS,ZYGOSITY_PERCENT_READ," +
                        "ZYGOSITY_POSS_ERROR,ZYGOSITY_REF_ALLELE,ZYGOSITY_NUM_ALLELE,ZYGOSITY_IN_PSEUDO,QUALITY_SCORE)\n" +
                        "VALUES (?,?,?,?,?,?,?," +
                        "?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.INTEGER, Types.INTEGER, Types.INTEGER,Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR,Types.VARCHAR, Types.INTEGER,Types.VARCHAR, Types.INTEGER}, 10000);
        bsu.compile();
        for(VariantSampleDetail v: sampleData ) {
            bsu.update(v.getId(), v.getSource(), v.getSampleId(),v.getDepth(),v.getVariantFrequency(),v.getZygosityStatus(),v.getZygosityPercentRead(),
                    v.getZygosityPossibleError(),v.getZygosityRefAllele(),v.getZygosityNumberAllele(),v.getZygosityInPseudo(),v.getQualityScore());
        }
        bsu.flush();
        // compute nr of rows affected
        int totalRowsAffected = 0;
        for( int rowsAffected: bsu.getRowsAffected() ) {
            totalRowsAffected += rowsAffected;
        }
        return totalRowsAffected;
    }
    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getDataSource("Variant");
    }
    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public BufferedWriter getLogWriter() {
        return logWriter;
    }

    public void setLogWriter(BufferedWriter logWriter) {
        this.logWriter = logWriter;
    }

    public void logStatusMsg(String msg) {
        this.logStatus.info(msg);
    }
}
