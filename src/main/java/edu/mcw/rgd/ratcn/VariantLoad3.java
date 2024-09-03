package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import edu.mcw.rgd.util.Zygosity;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.zip.GZIPInputStream;

/**
 * @author mtutaj
 * @since 09/15/2014
 * <p>
 * Program to process and load variant data files that are in common format 2 and load them into the database table VARIANT.
 * Indels are also supported.
 */
public class VariantLoad3 extends VariantProcessingBase {

    private String version;
    private long rowsInserted;
    private long rowsAlreadyInRgd; // could be non-zero only if VERIFY_IF_UNIQUE=true
    private long rowsUpdated = 0;

    private String LOG_FILE;

    // default is false: variants are inserted without any checking
    // if true, VARIANT table is queried if the given variant already exists in RGD
    boolean VERIFY_IF_IN_RGD = false;

    Sample sample;

    // per sample stats
    int dbSnpRowCount = 0;
    int novelRowCount = 0;
    int badVariants = 0; // 'variants' present in incoming data that had been called 0 times

    private SampleDAO sampleDAO = new SampleDAO();
    private VariantDAO dao = new VariantDAO();
    private Zygosity zygosity = new Zygosity();
    List<Variant> variants = new ArrayList<>();
    private RGDManagementDAO managementDAO = new RGDManagementDAO();
    private String chr;
    private List<VariantMapData> loaded_cache = new ArrayList<>();
    HashMap<Long,List<VariantMapData>> loadedData = new HashMap<>(); // grouped by pos
    private boolean clinvar = false;
    private boolean processOneVariantAtATime = false; // good for debugging

    public VariantLoad3() throws Exception {
        dao.setDataSource(getDataSource());
        sampleDAO.setDataSource(getDataSource());
    }

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        VariantLoad3 instance = (VariantLoad3) (bf.getBean("variantLoad3"));

        // Check incoming arguments and set Config with those the user can override
        List<String> sampleIds = new ArrayList<>();
        List<String> inputFiles = new ArrayList<>();

        for( int i=0; i<args.length; i++ ) {
            String arg = args[i];
            switch (arg) {
                case "--sampleId":
                case "-s":
                    sampleIds.add(args[++i]);
                    break;
                case "--inputFile":
                case "-i":
                    inputFiles.add(args[++i]);
                    break;
                case "--logFileName":
                case "-l":
                    String logFileName = args[++i];
                    instance.LOG_FILE = instance.LOG_FILE.replace("variantIlluminaLoad", logFileName);
                    break;
                case "--verifyIfInRgd":
                case "-v":
                    instance.VERIFY_IF_IN_RGD = true;
                    break;
                case "--chr":
                    instance.chr = args[++i];
                    break;
                case "--clinvar":
                    instance.clinvar = true;
                    break;
            }
        }

        if( sampleIds.isEmpty() || sampleIds.size()!=inputFiles.size() ) {
            System.out.println("Invalid arguments");
            return;
        }

        for( int i=0; i<sampleIds.size(); i++ ) {
            instance.run(sampleIds.get(i), inputFiles.get(i));
        }
    }

    public void run(String sampleId, String inputFile) throws Exception {

        sample = sampleDAO.getSample(Integer.parseInt(sampleId));

        System.out.println("OPTIONS:");
        System.out.println("  SampleId = "+sampleId);
        System.out.println("  InputFile = "+inputFile);
        System.out.println("  VerifyIfInRgd = "+VERIFY_IF_IN_RGD);
        System.out.println("  Gender = "+sample.getGender());
        System.out.println("  Chr = "+(chr==null ? " any " : chr));

        String logFile = LOG_FILE +"_"+sampleId+".log";
        setLogWriter(new BufferedWriter(new FileWriter(logFile)));
        System.out.println("  LOG_FILE = " + logFile);
        System.out.println();

        rowsInserted = 0;
        rowsUpdated = 0;
        rowsAlreadyInRgd = 0;
        run(inputFile);
    }

    public void run(String inputFile) throws Exception {

        System.out.println(getVersion());
        this.insertSystemLogMessage("variantLoading3", "Started for Sample "+sample.getId()+" with "+inputFile);

        File file = new File(inputFile);
        if( !file.exists() ) {
            this.getLogWriter().write("WARNING : Input file " + inputFile + " does not exist!\n");
        }

        long timestamp1 = System.currentTimeMillis();
        processVariants(file);

        System.out.println("Rows inserted: dbSnp="+dbSnpRowCount+", novel="+novelRowCount+", elapsed "+Utils.formatElapsedTime(timestamp1, System.currentTimeMillis()));
        this.insertSystemLogMessage("variantLoading", "Loaded "+(dbSnpRowCount+novelRowCount)+" variants for Sample "+sample.getId());
        if( badVariants>0 ) {
            String msg = "NOTE: "+badVariants+" variants skipped because they have been called 0 times!";
            System.out.println(msg);
            this.insertSystemLogMessage("variantLoading", msg += " - Sample "+sample.getId());
        }

        System.out.println("rowsInserted="+rowsInserted);
        System.out.println("rowsUpdated="+rowsUpdated);

        if( rowsAlreadyInRgd>0 ) {
            System.out.println("rows already in Rgd="+rowsAlreadyInRgd);
            getLogWriter().write("rows already in Rgd="+rowsAlreadyInRgd+"\n");
        }
        getLogWriter().flush();

        this.insertSystemLogMessage("variantLoading", "Ended for Sample "+sample.getId());
        System.out.println("  Program finished normally - "+new Date()+"\n");
        getLogWriter().write("  Program finished normally - "+new Date()+"\n");
        getLogWriter().close();
    }

    // DB SNP Variant processing first
    private void processVariants(File file) throws Exception {

        dbSnpRowCount = 0;
        novelRowCount = 0;
        badVariants = 0;
        rowsAlreadyInRgd = 0;

        // the file must has extension '.txt' or '.txt.gz'
        long rowsAlreadyInRgd0 = rowsAlreadyInRgd;

        String msg = " processing "+file.getName()+" - "+new Date()+"\n";
        //getLogWriter().write(msg);
        System.out.print(msg);

        // open the file
        BufferedReader reader;
        if( file.getName().endsWith(".txt.gz") ) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        } else {
            reader = new BufferedReader(new FileReader(file));
        }

        // process all other lines
        String line;
        while((line=reader.readLine())!=null ) {
            // skip comment line
            if( line.startsWith("#") )
                continue;
            processLine(line);
        }

        // cleanup
        reader.close();
        if(clinvar)
            saveClinvarVariants();
        else
            saveVariants();

        long rowsSkipped = rowsAlreadyInRgd - rowsAlreadyInRgd0;
        if( rowsSkipped>0 ) {
            System.out.println("    rows skipped (already in RGD) ="+rowsSkipped);
        }
    }

    void processLine(String line) throws Exception {
        //getLogWriter().write(line+"\n");

        String[] cols = line.split("[\t]", -1);
        String chr = cols[0];

        // chr filter
        if( this.chr!=null ) {
            if( !this.chr.equals(chr) ) {
                return; // line skipped: different chromosome
            }
        }

        int position = Integer.parseInt(cols[1]);
        String refSeq = cols[2]; // reference nucleotide for snvs (or ref sequence for indels)
        String varSeq = cols[3]; // variant nucleotide for snvs (or var sequence for indels)
        boolean isSnv = !Utils.isStringEmpty(refSeq) && !Utils.isStringEmpty(varSeq);

        if( isSnv ) {
            if (!alleleIsValid(refSeq)) {
                System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
                return;
            }
            if (!alleleIsValid(varSeq)) {
                System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
                return;
            }
        }

        // NOTE: for snvs, only ACGT counts are provided
        //    for indels, only allele count is provided
        int alleleDepth = parseInt(cols, 12); // from AD field: how many times allele was called
        int readDepth = parseInt(cols, 14); // from AD field: how many times all alleles were called
        int readCountA = parseInt(cols, 5);
        int readCountC = parseInt(cols, 6);
        int readCountG = parseInt(cols, 7);
        int readCountT = parseInt(cols, 8);

        int totalDepth = 0;
        String totalDepthStr = cols[9];
        if( totalDepthStr==null || totalDepthStr.isEmpty() ) {
            if( isSnv )
                totalDepth = readCountA + readCountC + readCountG + readCountT;
            else
                totalDepth = readDepth;
        } else
            totalDepth = Integer.parseInt(totalDepthStr);

        // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
        int qualityScore = 0;
        if( totalDepth>0 ) {
            qualityScore = (100 * readDepth + totalDepth/2)/ totalDepth;
        }

        String hgvsName = cols[10];
        String varRgdId = cols[11];



        Variant v = new Variant();
        v.setChromosome(chr);
        v.setReferenceNucleotide(refSeq);
        v.setStartPos(position);
        v.setDepth(readDepth);
        v.setVariantFrequency(alleleDepth);
        v.setVariantNucleotide(varSeq);
        v.setSampleId(sample.getId());
        v.setQualityScore(qualityScore);
        v.setHgvsName(hgvsName);
        if( !varRgdId.isEmpty() )
            v.setRgdId(Integer.parseInt(varRgdId));
        v.setVariantType(determineVariantType(refSeq, varSeq));
        v.setGenicStatus(isGenic(sample.getMapKey(), chr, position)?"GENIC":"INTERGENIC");
        if( !isSnv ) {
            v.setPaddingBase(cols[15]);
        }

        // Determine the ending position
        long endPos = 0;
        if( isSnv ) {
            endPos = v.getStartPos() + 1;
        } else {
            // insertions
            if( Utils.isStringEmpty(refSeq) ) {
                endPos = v.getStartPos();
            }
            // deletions
            else if( Utils.isStringEmpty(varSeq) ) {
                endPos = v.getStartPos() + refSeq.length();
            } else {
                System.out.println("Unexpected var type");
            }
        }
        v.setEndPos(endPos);

        int score;
        if( isSnv ) {
            score = zygosity.computeVariant(readCountA, readCountC, readCountG, readCountT, sample.getGender(), v);
        } else {
            // parameter tweaking for indels
            zygosity.computeZygosityStatus(alleleDepth, readDepth, sample.getGender(), v);

            // compute zygosity ref allele, if possible
            if( refSeq.equals("A") ) {
                v.setZygosityRefAllele(readCountA>0 ? "Y" : "N");
            }
            else if( refSeq.equals("C") ) {
                v.setZygosityRefAllele(readCountC>0 ? "Y" : "N");
            }
            else if( refSeq.equals("G") ) {
                v.setZygosityRefAllele(readCountG>0 ? "Y" : "N");
            }
            else if( refSeq.equals("T") ) {
                v.setZygosityRefAllele(readCountT>0 ? "Y" : "N");
            }

            if( alleleDepth==0 )
                score = 0;
            else
                score = v.getZygosityPercentRead();
        }
        if( score==0 ) {
            badVariants++;
            return;
        }

        String rsId = cols[4];
        v.setRsId(rsId);
        if( rsId!=null && !rsId.isEmpty() )
            dbSnpRowCount++;
        else
            novelRowCount++;

        variants.add(v);
        //if( (rowsInserted+rowsUpdated)%1000 == 0 ) {
            //System.out.println("VARIANTS PROCESSED: "+variants.size());
//            getLogWriter().write("CHECKPOINT: inserted=" + rowsInserted+", updated="+rowsUpdated+"\n");
  //          getLogWriter().flush();
    //    }

        if( processOneVariantAtATime ) {
            saveVariant(v);
        }
    }

    // allele nucleotides must be from the set: 'A','C','G','T','N'
    boolean alleleIsValid(String allele) {
        for( int i=0; i<allele.length(); i++ ) {
            char c = allele.charAt(i);
            if( c=='A' || c=='C' || c=='G' || c=='T' || c=='N' || c=='-' )
                continue;
            return false;
        }
        return true;
    }

    // if col is invalid, 0 is returned
    int parseInt(String[] arr, int col) {
        if( col<0 || col>=arr.length )
            return 0;
        String s = arr[col];
        if( s.isEmpty() )
            return 0;
        return Integer.parseInt(s);
    }

    /**
     * save variant into database, table VARIANT
     */
    void saveVariant(Variant variant) throws Exception {

        int speciesKey=SpeciesType.getSpeciesTypeKeyForMap(sample.getMapKey());

        VariantMapData mapData = new VariantMapData();
        mapData.setMapKey(sample.getMapKey());
        mapData.setReferenceNucleotide(variant.getReferenceNucleotide());
        mapData.setVariantNucleotide(variant.getVariantNucleotide());
        mapData.setVariantType(variant.getVariantType());
        mapData.setSpeciesTypeKey(speciesKey);
        mapData.setChromosome(variant.getChromosome());
        mapData.setPaddingBase(variant.getPaddingBase());
        mapData.setStartPos(variant.getStartPos());
        mapData.setEndPos(variant.getEndPos());
        mapData.setGenicStatus(variant.getGenicStatus());
        mapData.setRsId(variant.getRsId());

        long id = 0;

        List<VariantMapData> maps = loadedData.get(mapData.getStartPos());
        if( maps==null ) {
            maps = getVariants(sample.getMapKey(), (int) variant.getStartPos());
            loadedData.put(mapData.getStartPos(), maps);
        }

        if( maps!=null ) {

            for(VariantMapData v: maps){
                if(v.getEndPos() == mapData.getEndPos()
                        && v.getChromosome().equalsIgnoreCase(mapData.getChromosome())
                        && Utils.stringsAreEqualIgnoreCase(v.getReferenceNucleotide(), mapData.getReferenceNucleotide())
                        && Utils.stringsAreEqualIgnoreCase(v.getVariantType(), mapData.getVariantType())
                        && Utils.stringsAreEqualIgnoreCase(v.getVariantNucleotide(), mapData.getVariantNucleotide() ) ) {

                    id = v.getId();
                    mapData.setId(id);
                }
            }
        } else {
            id = variant.getRgdId();
        }

        VariantSampleDetail sampleDetail = new VariantSampleDetail();
        sampleDetail.setSampleId(sample.getId());
        sampleDetail.setZygosityStatus(variant.getZygosityStatus());
        sampleDetail.setZygosityPercentRead(variant.getZygosityPercentRead());
        sampleDetail.setZygosityRefAllele(variant.getZygosityRefAllele());
        sampleDetail.setZygosityNumberAllele(variant.getZygosityNumberAllele());
        sampleDetail.setVariantFrequency(variant.getVariantFrequency());
        sampleDetail.setZygosityInPseudo(variant.getZygosityInPseudo());
        sampleDetail.setDepth(variant.getDepth());
        sampleDetail.setQualityScore(variant.getQualityScore());
        sampleDetail.setId(mapData.getId());

        if(id == 0 ) {
            RgdId r = managementDAO.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by Variant pipeline", speciesKey);
            mapData.setId(r.getRgdId());

            // insert new data to cache
            List<VariantMapData> mdata = loadedData.get(mapData.getStartPos());
            if (mdata == null) {
                mdata = new ArrayList<>();
                loadedData.put(mapData.getStartPos(), mdata);
            }
            mdata.add(mapData);

            insertVariantRgdId(mapData);
            insertVariant(mapData);
            insertVariantMapData(mapData);
            insertVariantSample(sampleDetail);

            rowsInserted++;
        } else {
            rowsAlreadyInRgd++;

            List<VariantSampleDetail> sampleDetailInRgd = getVariantSampleDetail((int)id, sample.getId());
            if( sampleDetailInRgd.isEmpty() ) {
                insertVariantSample(sampleDetail);
            }
        }


        variants.clear();
    }

    /**
     * save variant into database, table VARIANT
     */
    public void saveVariants() throws Exception {

        int speciesKey=SpeciesType.getSpeciesTypeKeyForMap(sample.getMapKey());

        if( loadedData.isEmpty() ) {
            loaded_cache = getVariants(speciesKey, sample.getMapKey(), chr);
            // group variants by chr and start pos to make them searchable using keys
            for (VariantMapData data : loaded_cache) {
                List<VariantMapData> mdata = loadedData.get(data.getStartPos());
                if (mdata == null) {
                    mdata = new ArrayList<>();
                    loadedData.put(data.getStartPos(), mdata);
                }
                mdata.add(data);
            }
            loaded_cache.clear();
        }

        Set<Integer> rgdIdsWithSampleDetail = new HashSet<>(getRgdIdsWithSampleDetail(sample.getId()));

        List<VariantSampleDetail> sampleDetailForUpdate = new ArrayList<>();

        System.out.println("Loaded from Variant file: " + variants.size());
        System.out.println("Already in RGD : " + loadedData.size());
        for (Variant variant : variants) {
            VariantMapData mapData = new VariantMapData();
            mapData.setMapKey(sample.getMapKey());
            mapData.setReferenceNucleotide(variant.getReferenceNucleotide());
            mapData.setVariantNucleotide(variant.getVariantNucleotide());
            mapData.setVariantType(variant.getVariantType());
            mapData.setSpeciesTypeKey(speciesKey);
            mapData.setChromosome(variant.getChromosome());
            mapData.setPaddingBase(variant.getPaddingBase());
            mapData.setStartPos(variant.getStartPos());
            mapData.setEndPos(variant.getEndPos());
            mapData.setGenicStatus(variant.getGenicStatus());
            mapData.setRsId(variant.getRsId());

            long id = 0;

            List<VariantMapData> maps = loadedData.get(mapData.getStartPos());
            if( maps!=null ) {

                for(VariantMapData v: maps){
                    if(v.getEndPos() == mapData.getEndPos()
                            && v.getChromosome().equalsIgnoreCase(mapData.getChromosome())
                            && Utils.stringsAreEqualIgnoreCase(v.getReferenceNucleotide(), mapData.getReferenceNucleotide())
                            && Utils.stringsAreEqualIgnoreCase(v.getVariantType(), mapData.getVariantType())
                            && Utils.stringsAreEqualIgnoreCase(v.getVariantNucleotide(), mapData.getVariantNucleotide() ) ) {

                        id = v.getId();
                        mapData.setId(id);
                    }
                }
            } else {
                id = variant.getRgdId();
            }

            VariantSampleDetail sampleDetail = new VariantSampleDetail();
            sampleDetail.setSampleId(sample.getId());
            sampleDetail.setZygosityStatus(variant.getZygosityStatus());
            sampleDetail.setZygosityPercentRead(variant.getZygosityPercentRead());
            sampleDetail.setZygosityRefAllele(variant.getZygosityRefAllele());
            sampleDetail.setZygosityPossibleError(variant.getZygosityPossibleError());
            sampleDetail.setZygosityNumberAllele(variant.getZygosityNumberAllele());
            sampleDetail.setVariantFrequency(variant.getVariantFrequency());
            sampleDetail.setZygosityInPseudo(variant.getZygosityInPseudo());
            sampleDetail.setDepth(variant.getDepth());
            sampleDetail.setQualityScore(variant.getQualityScore());

            if(id == 0 ) {
                RgdId r = managementDAO.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by Variant pipeline", speciesKey);
                id = r.getRgdId();
                mapData.setId(id);
                varBatch.add(mapData);

                // insert new data to cache
                List<VariantMapData> mdata = loadedData.get(mapData.getStartPos());
                if (mdata == null) {
                    mdata = new ArrayList<>();
                    loadedData.put(mapData.getStartPos(), mdata);
                }
                mdata.add(mapData);
                rowsInserted++;
            } else {
                rowsAlreadyInRgd++;
                sampleDetail.setId(id);
                sampleDetailForUpdate.add(sampleDetail);
            }

            if( !rgdIdsWithSampleDetail.contains((int)id) ) {
                sampleDetail.setId(id);
                sampleBatch.add(sampleDetail);
                rgdIdsWithSampleDetail.add((int)id);
            }

        }

        if( !sampleDetailForUpdate.isEmpty() ) {
            updateVariantSample(sampleDetailForUpdate);
        }

        insertVariantRgdIds(varBatch);
        insertVariants(varBatch);
        insertVariantMapData(varBatch);
        insertVariantSample(sampleBatch);
        varBatch.clear();
        sampleBatch.clear();
        loadedData.clear();
        variants.clear();
    }

    public void saveClinvarVariants() throws Exception {

        List<edu.mcw.rgd.ratcn.Variant> variantList = getVariantObjects(SpeciesType.HUMAN);
        HashMap<Long,VariantMapData> data = new HashMap<>();
        HashMap<Long, edu.mcw.rgd.ratcn.Variant> varMap = new HashMap<>();
        if(loaded_cache.size()== 0) {
            loaded_cache = getVariants(SpeciesType.HUMAN, sample.getMapKey(), chr);
            // group variants by rgd id to make them searchable using keys
            for (VariantMapData variantMapData : loaded_cache) {
                data.put(variantMapData.getId(), variantMapData);
            }
        }
        for(edu.mcw.rgd.ratcn.Variant v:variantList){
            varMap.put(v.getId(),v);
        }

        System.out.println("Loaded from File: " + variants.size());
        System.out.println("Loaded from Variant : " + loaded_cache.size());
        System.out.println("Loaded from Variant objects : " + varMap.size());
        for (Variant variant : variants) {
            VariantMapData mapData = new VariantMapData();
            mapData.setMapKey(sample.getMapKey());
            mapData.setReferenceNucleotide(variant.getReferenceNucleotide());
            mapData.setVariantNucleotide(variant.getVariantNucleotide());
            mapData.setVariantType(variant.getVariantType());
            mapData.setSpeciesTypeKey(SpeciesType.HUMAN);
            mapData.setChromosome(variant.getChromosome());
            mapData.setPaddingBase(variant.getPaddingBase());
            mapData.setStartPos(variant.getStartPos());
            mapData.setEndPos(variant.getEndPos());
            mapData.setGenicStatus(variant.getGenicStatus());
            long id = variant.getRgdId();
            mapData.setId(id);
            VariantSampleDetail sampleDetail = new VariantSampleDetail();
            sampleDetail.setSampleId(sample.getId());
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


            if(varMap.containsKey(id)) {
                if(data.containsKey(id))
                    System.out.println("Exists");
                else {
                    varMapBatch.add(mapData);
                    sampleBatch.add(sampleDetail);
                }
            }else {
                varBatch.add(mapData);
                varMapBatch.add(mapData);
                sampleBatch.add(sampleDetail);
            }


        }
        insertVariantRgdIds(varBatch);
        insertVariants(varBatch);
        insertVariantMapData(varMapBatch);
        insertVariantSample(sampleBatch);
        varBatch.clear();
        sampleBatch.clear();
        loaded_cache.clear();
        variants.clear();
        insertClinvarIds();
    }
    List<VariantMapData> varBatch = new ArrayList<VariantMapData>();
    List<VariantMapData> varMapBatch = new ArrayList<>();
    List<VariantSampleDetail> sampleBatch=new ArrayList<>();


    String determineVariantType(String refSeq, String varSeq) {

        // handle insertions
        if( refSeq.length()==0 )
            return "insertion";

        // handle deletions
        if( varSeq.length()==0 )
            return "deletion";

        // handle snv
        return "snv";
    }

    boolean isGenic(int mapKey, String chr, int pos) throws Exception {

        GeneCache geneCache = geneCacheMap.get(chr);
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(chr, geneCache);
            geneCache.loadCache(mapKey, chr, DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(pos);
        return !geneRgdIds.isEmpty();
    }

    Map<String, GeneCache> geneCacheMap = new HashMap<>();


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public String getLOG_FILE() {
        return LOG_FILE;
    }

    public void setLOG_FILE(String LOG_FILE) {
        this.LOG_FILE = LOG_FILE;
    }
}
