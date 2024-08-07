package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GenomicElementDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.dao.spring.VariantMapper;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.process.mapping.MapManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 09/15/2014
 * <p>
 * Program to process and load variant data files that are in common format 2 and load them into the database table VARIANT.
 * Indels are also supported.
 */
public class VariantRatLoaderFromDb extends VariantProcessingBase {

    private SampleDAO sampleDAO = new SampleDAO();
    private VariantDAO dao = new VariantDAO();
    private RGDManagementDAO managementDAO = new RGDManagementDAO();
    List<VariantMapData> varBatch = new ArrayList<>();
    List<VariantMapData> varMapBatch = new ArrayList<>();
    List<VariantSampleDetail> sampleBatch = new ArrayList<>();
    HashMap<Long,List<VariantMapData>> loadedData = new HashMap<>();
    List<VariantMapData> loaded = new ArrayList<>();

    public VariantRatLoaderFromDb() throws Exception {
        dao.setDataSource(getDataSource());
        sampleDAO.setDataSource(getDataSource());
    }

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        VariantRatLoaderFromDb instance = (VariantRatLoaderFromDb) (bf.getBean("variantRatLoader"));


        // Check incoming arguments and set Config with those the user can override
        List<String> sampleIds = new ArrayList<>();
        List<String> chrs = new ArrayList<>();

        for( int i=0; i<args.length; i++ ) {
            String arg = args[i];
            switch (arg) {
                case "--sampleId":
                case "-s":
                    sampleIds.add(args[++i]);
                    break;
                case "--chr":
                    chrs.add(args[++i]);
                    break;
            }
        }


       if( sampleIds.isEmpty() ) {
            System.out.println("Invalid arguments");
            return;
        }

        for(String chr: chrs) {
            for(String sampleId: sampleIds) {
                if(sampleId.equalsIgnoreCase("1") || sampleId.equalsIgnoreCase("2"))
                    instance.runClinVar(sampleId, chr);
                else instance.run(sampleId,chr);
                instance.loaded.clear();
            }
        }


    }



    public void run(String sampleId, String chr) throws Exception {
        Sample s = sampleDAO.getSample(Integer.parseInt(sampleId));
        int speciesKey = MapManager.getInstance().getMap(s.getMapKey()).getSpeciesTypeKey();

        System.out.println("Running for Chromosome: " + chr + "and Sample: "+ sampleId);
        List<Variant> variants = getVariants(s.getId(),chr);
       if(loaded.size()== 0) {
           loaded = getVariants(speciesKey, s.getMapKey(), chr);
           List<VariantMapData> mdata = new ArrayList<>();
           // group variants by chr and start pos to make them searchable using keys
           for (VariantMapData data : loaded) {
               mdata = loadedData.get(data.getStartPos());
               if (mdata == null) {
                   mdata = new ArrayList<>();
               }
               mdata.add(data);
               loadedData.put(data.getStartPos(), mdata);
           }
       }


        System.out.println("Loaded from Variant Ratcn: " + variants.size());
        System.out.println("Loaded from Variant : " + loaded.size());
        for (Variant variant : variants) {
            VariantMapData mapData = new VariantMapData();
            mapData.setMapKey(s.getMapKey());
            mapData.setReferenceNucleotide(variant.getReferenceNucleotide());
            mapData.setVariantNucleotide(variant.getVariantNucleotide());
            mapData.setVariantType(variant.getVariantType());
            mapData.setSpeciesTypeKey(speciesKey);
            mapData.setChromosome(variant.getChromosome());
            mapData.setPaddingBase(variant.getPaddingBase());
            mapData.setStartPos(variant.getStartPos());
            mapData.setEndPos(variant.getEndPos());
            mapData.setGenicStatus(variant.getGenicStatus());
            long id = 0;
            if(loaded.size() != 0 && loadedData.keySet().contains(mapData.getStartPos())){
                List<VariantMapData> maps = loadedData.get(mapData.getStartPos());
                for(VariantMapData v: maps){
                    if(speciesKey == SpeciesType.HUMAN && v.getId() == variant.getRgdId()) {
                        id = v.getId();
                        mapData.setId(id);
                    }
                    else if(v.getEndPos() == mapData.getEndPos()
                            && ((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || ( v.getReferenceNucleotide() != null && v.getReferenceNucleotide().equalsIgnoreCase(mapData.getReferenceNucleotide())))
                            && v.getVariantType().equalsIgnoreCase(mapData.getVariantType())
                            && ((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || (v.getVariantNucleotide() != null && v.getVariantNucleotide().equalsIgnoreCase(mapData.getVariantNucleotide()))) ) {
                        id = v.getId();
                        mapData.setId(id);
                    }
                }
            }
            if(id == 0 ) {
                if (speciesKey != SpeciesType.HUMAN) {
                    RgdId r = managementDAO.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by Variant pipeline", speciesKey);
                    mapData.setId(r.getRgdId());
                    varBatch.add(mapData);
                } else {
                    mapData.setId(variant.getRgdId());
                    varBatch.add(mapData);
                }
            }
            VariantSampleDetail sampleDetail = new VariantSampleDetail();
            sampleDetail.setSampleId(s.getId());
            sampleDetail.setZygosityStatus(variant.getZygosityStatus());
            sampleDetail.setZygosityPercentRead(variant.getZygosityPercentRead());
            sampleDetail.setZygosityRefAllele(variant.getZygosityRefAllele());
            sampleDetail.setZygosityNumberAllele(variant.getZygosityNumberAllele());
            sampleDetail.setVariantFrequency(variant.getVariantFrequency());
            sampleDetail.setZygosityInPseudo(variant.getZygosityInPseudo());
            sampleDetail.setZygosityPossibleError(variant.getZygosityPossibleError());
            sampleDetail.setDepth(variant.getDepth());
            sampleDetail.setQualityScore(variant.getQualityScore());
            sampleDetail.setId(mapData.getId());
            sampleBatch.add(sampleDetail);


        }


        insertVariants(varBatch);
        insertVariantMapData(varBatch);
        insertVariantSample(sampleBatch);
        varBatch.clear();
        sampleBatch.clear();
        loadedData.clear();
        variants.clear();
    }


    public void runClinVar(String sampleId, String chr) throws Exception {
        Sample s = sampleDAO.getSample(Integer.parseInt(sampleId));
        int speciesKey = MapManager.getInstance().getMap(s.getMapKey()).getSpeciesTypeKey();

        System.out.println("Running for Chromosome: " + chr + "and Sample: "+ sampleId);
        List<Variant> variants = getVariants(s.getId(),chr);
        List<edu.mcw.rgd.ratcn.Variant> variantList = getVariantObjects(SpeciesType.HUMAN);
        HashMap<Long,VariantMapData> data = new HashMap<>();
        HashMap<Long, edu.mcw.rgd.ratcn.Variant> varMap = new HashMap<>();
        if(loaded.size()== 0) {
            loaded = getVariants(speciesKey, s.getMapKey(), chr);
            // group variants by rgd id to make them searchable using keys
            for (VariantMapData variantMapData : loaded) {
                data.put(variantMapData.getId(), variantMapData);
            }
        }
        for(edu.mcw.rgd.ratcn.Variant v:variantList){
            varMap.put(v.getId(),v);
        }

        System.out.println("Loaded from Variant Ratcn: " + variants.size());
        System.out.println("Loaded from Variant : " + loaded.size());
        System.out.println("Loaded from Variant objects : " + varMap.size());
        for (Variant variant : variants) {
            VariantMapData mapData = new VariantMapData();
            mapData.setMapKey(s.getMapKey());
            mapData.setReferenceNucleotide(variant.getReferenceNucleotide());
            mapData.setVariantNucleotide(variant.getVariantNucleotide());
            mapData.setVariantType(variant.getVariantType());
            mapData.setSpeciesTypeKey(speciesKey);
            mapData.setChromosome(variant.getChromosome());
            mapData.setPaddingBase(variant.getPaddingBase());
            mapData.setStartPos(variant.getStartPos());
            mapData.setEndPos(variant.getEndPos());
            mapData.setGenicStatus(variant.getGenicStatus());
            long id = 0;
            if(loaded.size() != 0 && data.keySet().contains(variant.getRgdId())){
                VariantMapData v = data.get(variant.getRgdId());
              if(v.getEndPos() == mapData.getEndPos()
                            && ((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || ( v.getReferenceNucleotide() != null && v.getReferenceNucleotide().equalsIgnoreCase(mapData.getReferenceNucleotide())))
                            && v.getVariantType().equalsIgnoreCase(mapData.getVariantType())
                            && ((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || (v.getVariantNucleotide() != null && v.getVariantNucleotide().equalsIgnoreCase(mapData.getVariantNucleotide()))) ) {
                        id = v.getId();
                        mapData.setId(id);
                    }
            }
            if(id == 0 ) {
                    mapData.setId(variant.getRgdId());
                if(varMap.keySet().contains(variant.getRgdId())) {
                    edu.mcw.rgd.ratcn.Variant v = varMap.get(variant.getRgdId());
                    if(((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || ( v.getReferenceNucleotide() != null && v.getReferenceNucleotide().equalsIgnoreCase(mapData.getReferenceNucleotide())))
                    && v.getVariantType().equalsIgnoreCase(mapData.getVariantType())
                            && ((v.getVariantNucleotide() == null && mapData.getVariantNucleotide() == null )
                            || (v.getVariantNucleotide() != null && v.getVariantNucleotide().equalsIgnoreCase(mapData.getVariantNucleotide()))) )
                    varMapBatch.add(mapData);
                    else {
                        varBatch.add(mapData);
                        varMapBatch.add(mapData);
                   }
                 }else {
                    varBatch.add(mapData);
                    varMapBatch.add(mapData);
                }
            }
            VariantSampleDetail sampleDetail = new VariantSampleDetail();
            sampleDetail.setSampleId(s.getId());
            sampleDetail.setZygosityStatus(variant.getZygosityStatus());
            sampleDetail.setZygosityPercentRead(variant.getZygosityPercentRead());
            sampleDetail.setZygosityPossibleError(variant.getZygosityPossibleError());
            sampleDetail.setZygosityRefAllele(variant.getZygosityRefAllele());
            sampleDetail.setZygosityNumberAllele(variant.getZygosityNumberAllele());
            sampleDetail.setVariantFrequency(variant.getVariantFrequency());
            sampleDetail.setZygosityInPseudo(variant.getZygosityInPseudo());
            sampleDetail.setDepth(variant.getDepth());
            sampleDetail.setQualityScore(variant.getQualityScore());
            sampleDetail.setId(mapData.getId());
            sampleBatch.add(sampleDetail);
        }
        insertVariants(varBatch);
        insertVariantMapData(varMapBatch);
        insertVariantSample(sampleBatch);
        varBatch.clear();
        sampleBatch.clear();
        loadedData.clear();
        variants.clear();
        insertClinvarIds();
    }


    public List<Variant> getVariants(int sampleId, String chr) {

        String varTable = "";
        if( sampleId<100 ) {
            varTable = "variant_clinvar";
        }else if( sampleId>=6000 && sampleId<=6999 ) {
            varTable = "variant_dog";
        } else varTable =  "variant_old";

        String sql = "SELECT * FROM "+varTable+" where sample_id = ? and chromosome=?";// and start_pos between 88564465 and 177128931"; //183739492

        VariantMapper q = new VariantMapper(getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(sampleId,chr);
    }

    public List<VariantMapData> getVariants(int speciesKey, int mapKey,String chr) throws Exception{

        String sql = "SELECT * FROM variant v inner join variant_map_data vm on v.rgd_id = vm. rgd_id WHERE v.species_type_key = ? and vm.map_key = ? and vm.chromosome=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(speciesKey,mapKey,chr);
    }
    public List<edu.mcw.rgd.ratcn.Variant> getVariantObjects(int speciesKey) throws Exception{

        String sql = "SELECT * FROM variant v WHERE v.species_type_key = ?";
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

    List<String> getChromosomes(int sampleId) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM variant WHERE sample_id=? ";
        StringListQuery q = new StringListQuery(getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        return q.execute(new Object[]{sampleId});
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
        return DataSourceFactory.getInstance().getDataSource("Carpe");
    }

}