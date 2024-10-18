package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GenomicElementDAO;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.datamodel.GenomicElement;
import edu.mcw.rgd.datamodel.SpeciesType;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    VariantDAO vdao = new VariantDAO();

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
        String sql = "SELECT * FROM variant v inner join variant_map_data vm on v.rgd_id = vm. rgd_id WHERE v.species_type_key = ? and vm.map_key = ?";
        if(chr !=null && !chr.equals(""))
             sql +=   " and vm.chromosome=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        if(chr !=null && !chr.equals("")) {
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            return q.execute(speciesKey, mapKey, chr);
        }else return q.execute(speciesKey,mapKey);
    }

    public List<VariantMapData> getVariants(int mapKey, int startPos) throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vm on v.rgd_id = vm. rgd_id  WHERE vm.map_key=? AND vm.start_pos=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(mapKey, startPos);
    }

    public List<Variant> getVariantObjects(int speciesKey) throws Exception{
        String sql = "SELECT * FROM variant WHERE species_type_key = ?";
        VariantQuery q = new VariantQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(speciesKey);
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

    public int updateVariantSample(List<VariantSampleDetail> sampleData) throws Exception {
        BatchSqlUpdate bsu= new BatchSqlUpdate(this.getVariantDataSource(),
                "UPDATE variant_sample_detail " +
                        "SET ZYGOSITY_POSS_ERROR=? "+
                        "WHERE RGD_ID=? AND sample_id=?",
                new int[]{Types.VARCHAR,Types.INTEGER, Types.INTEGER}, 10000);
        bsu.compile();
        for(VariantSampleDetail v: sampleData ) {
            bsu.update(v.getZygosityPossibleError(),v.getId(), v.getSampleId());
        }
        bsu.flush();
        // compute nr of rows affected
        int totalRowsAffected = 0;
        for( int rowsAffected: bsu.getRowsAffected() ) {
            totalRowsAffected += rowsAffected;
        }
        return totalRowsAffected;
    }


    public void insertVariant(VariantMapData v)  throws Exception{
        SqlUpdate sql1 = new SqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant (" +
                        " RGD_ID,REF_NUC, VARIANT_TYPE, VAR_NUC, RS_ID, CLINVAR_ID, SPECIES_TYPE_KEY) " +
                        "VALUES (?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.INTEGER});
        sql1.compile();
        sql1.update(v.getId(), v.getReferenceNucleotide(), v.getVariantType(), v.getVariantNucleotide(), v.getRsId(), v.getClinvarId(), v.getSpeciesTypeKey());
    }

    public void insertVariantMapData(VariantMapData v)  throws Exception{
        SqlUpdate sql2 = new SqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_map_data (" +
                        " RGD_ID,CHROMOSOME,START_POS,END_POS,PADDING_BASE,GENIC_STATUS,MAP_KEY) " +
                        "VALUES (?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,Types.VARCHAR, Types.INTEGER});
        sql2.compile();
        sql2.update(v.getId(), v.getChromosome(), v.getStartPos(), v.getEndPos(), v.getPaddingBase(), v.getGenicStatus(), v.getMapKey());
    }

    public void insertVariantSample(VariantSampleDetail v) throws Exception {
        SqlUpdate bsu= new SqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_sample_detail (" +
                        " RGD_ID,SOURCE,SAMPLE_ID,TOTAL_DEPTH,VAR_FREQ,ZYGOSITY_STATUS,ZYGOSITY_PERCENT_READ," +
                        "ZYGOSITY_POSS_ERROR,ZYGOSITY_REF_ALLELE,ZYGOSITY_NUM_ALLELE,ZYGOSITY_IN_PSEUDO,QUALITY_SCORE)\n" +
                        "VALUES (?,?,?,?,?,?,?," +
                        "?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.INTEGER, Types.INTEGER, Types.INTEGER,Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR,Types.VARCHAR, Types.INTEGER,Types.VARCHAR, Types.INTEGER});
        bsu.compile();
        bsu.update(v.getId(), v.getSource(), v.getSampleId(),v.getDepth(),v.getVariantFrequency(),v.getZygosityStatus(),v.getZygosityPercentRead(),
                   v.getZygosityPossibleError(),v.getZygosityRefAllele(),v.getZygosityNumberAllele(),v.getZygosityInPseudo(),v.getQualityScore());
    }

    public List<VariantSampleDetail> getVariantSampleDetail(int rgdId, int sampleId) throws Exception{
        String sql = "SELECT * FROM variant_sample_detail  WHERE rgd_id=? AND sample_id=?";
        VariantSampleQuery q = new VariantSampleQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(rgdId, sampleId);
    }

    public List<Integer> getRgdIdsWithSampleDetail(int sampleId) throws Exception{
        String sql = "SELECT rgd_id FROM variant_sample_detail  WHERE sample_id=?";
        IntListQuery q = new IntListQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(sampleId);
    }

    public void insertClinvarIds() throws Exception{
        GenomicElementDAO gedao = new GenomicElementDAO();
        List<edu.mcw.rgd.ratcn.Variant> variants = getVariantObjects(SpeciesType.HUMAN);
        System.out.println(variants.size());
        HashMap<Integer,String> data = new HashMap<>();
        List<Integer> rgdIds = new ArrayList<>();
        String sql = "update variant set clinvar_id = ? where rgd_id = ?";
        BatchSqlUpdate su = new BatchSqlUpdate(getVariantDataSource(), sql,new int[]{Types.VARCHAR,Types.INTEGER}, 10000);
        su.compile();
        for(edu.mcw.rgd.ratcn.Variant v:variants){
            rgdIds.add(Long.valueOf(v.getId()).intValue());
            if(rgdIds.size() % 999 == 0) {
                System.out.println(rgdIds.size());
                List<GenomicElement> elementList = gedao.getElementsByRgdIds(rgdIds);
                rgdIds.clear();
                for(GenomicElement g:elementList) {
                    if (g.getSource() != null && g.getSource().equalsIgnoreCase("CLINVAR")) {
                        su.update(g.getSymbol(),g.getRgdId());
                    }
                }
                su.flush();
            }

        }
        System.out.println(rgdIds.size());
        List<GenomicElement> elementList = gedao.getElementsByRgdIds(rgdIds);
        rgdIds.clear();
        for(GenomicElement g:elementList) {
            if (g.getSource() != null && g.getSource().equalsIgnoreCase("CLINVAR")) {
                su.update(g.getSymbol(),g.getRgdId());
            }
        }
        su.flush();





    }
    public void insertVariantRgdIds(List<VariantMapData> vmds) throws Exception{
        BatchSqlUpdate sql = new BatchSqlUpdate(DataSourceFactory.getInstance().getCarpeNovoDataSource(),
                "INSERT INTO VARIANT_RGD_IDS (RGD_ID) VALUES (?)", new int[]{Types.INTEGER},5000);
        sql.compile();
        Map<Long,Integer> ids = new HashMap<>();
        for (VariantMapData vmd : vmds){
            long rgdId = vmd.getId();
            if (ids.get(rgdId)==null) {
                ids.put(rgdId,(int) rgdId);
                sql.update((int) rgdId);
            }
        }
        sql.flush();
    }

    public void insertVariantRgdId(VariantMapData v)  throws Exception{

        SqlUpdate sql1 = new SqlUpdate(this.getVariantDataSource(),
                "INSERT INTO VARIANT_RGD_IDS (RGD_ID) VALUES (?)",
                new int[]{Types.INTEGER});
        sql1.compile();
        sql1.update((int)v.getId());
    }
    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getCarpeNovoDataSource();
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
