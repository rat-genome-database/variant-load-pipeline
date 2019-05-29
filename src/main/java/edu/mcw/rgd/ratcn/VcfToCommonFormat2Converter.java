package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import edu.mcw.rgd.ratcn.convert.CommonFormat2Line;
import edu.mcw.rgd.ratcn.convert.CommonFormat2Writer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since 9/12/14
 * common format was 3 sets of tab delimited files as produced by early Illumina sequencers
 *   the drawback is that information is spread across 3 sets of files
 * common format 2 is a flat file format, designed by RGD
 *   it combines 3 sets of files used in common format 1 into one set of files
 */
public class VcfToCommonFormat2Converter extends VcfToCommonFormat2Base {

    private String version;

    String vcfFile = null;
    String outDir = null;
    String dbSnpSource = null;
    int mapKey = 0;
    boolean processVariantsSameAsRef = false;
    boolean compressOutputFile = false;
    boolean appendToOutputFile = false;
    boolean processLinesWithMissingADDP = false;

    // for every strain we have one genotype count map
    Object[] genotypeCountMaps;
    // how many lines have AD:DP missing?
    int linesWithADorDPmissing = 0;
    // how many lines have value of AD = "."
    int linesWithADdotted = 0;

    // key is strain name
    Map<String, CommonFormat2Writer> outputFiles = new HashMap<>();

    Map<String, Integer> variantCounts = new TreeMap<>();

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        VcfToCommonFormat2Converter converter = (VcfToCommonFormat2Converter) bf.getBean("vcf2commonFormat2");
        converter.parseCmdLine(args);

        //vcfFile = "/data/rat/toRGD/ACI_BNLX_BNSSN_BUF_F334_LE_M520_MR_WKY_WN_SNP_VQSR_99_5_filteredRemoved.vcf";

        long timestamp = System.currentTimeMillis();
        converter.run();
        System.out.println("----- OK --------");
        System.out.println("Finished! Elapsed "+ Utils.formatElapsedTime(timestamp, System.currentTimeMillis()));
    }

    void parseCmdLine(String[] args) throws Exception {

        // parse cmdline params
        for( int i=0; i<args.length; i++ ) {
            switch (args[i]) {
                case "--vcfFile":
                    vcfFile = args[++i];
                    break;
                case "--outDir":
                    outDir = args[++i];
                    break;
                case "--mapKey":
                    mapKey = Integer.parseInt(args[++i]);
                    edu.mcw.rgd.datamodel.Map map = MapManager.getInstance().getMap(mapKey);
                    if( map!=null ) {
                        dbSnpSource = map.getDbsnpVersion();
                    }
                    break;
                case "--processVariantsSameAsRef":
                    processVariantsSameAsRef = true;
                    break;
                case "--compressOutputFile":
                    compressOutputFile = true;
                    break;
                case "--appendToOutputFile":
                    appendToOutputFile = true;
                    break;
                case "--ADDP":
                    processLinesWithMissingADDP = true;
                    break;
            }
        }

        System.out.println(getVersion());
        System.out.println("  --vcfFile "+vcfFile);
        System.out.println("  --outDir  "+outDir);
        System.out.println("  --mapKey "+mapKey);
        System.out.println("  --dbSnpSource  "+dbSnpSource);
        System.out.println("  --processVariantsSameAsRef  " + processVariantsSameAsRef);
        System.out.println("  --compressOutputFile  " + compressOutputFile);
        System.out.println("  --appendToOutputFile  " + appendToOutputFile);
        System.out.println();
    }

    public void run() throws Exception {

        BufferedReader reader = Utils.openReader(vcfFile);
        String line;

        // skip all header lines, starting with '##'
        while( (line=reader.readLine())!=null && line.startsWith("##") ) {
        }

        // real all strain names
        // f.e. the line below contains data for only one strain, SHR
        //  0       1   2     3  4    5         6      7       8     9
        // #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SHR
        String[] header = null;
        int strainCount = 0;
        if( line!=null && line.startsWith("#") ) {
            header = line.substring(1).split("[\\t]", -1);
            strainCount = header.length - 9;
        }

        init(strainCount);

        // process all lines
        while( (line=reader.readLine())!=null ) {
            processLine(line, strainCount, header);
        }
        reader.close();

        closeOutputFiles();

        dumpSummaries();
    }

    /**
     * init genotype count maps and establish database connection
     * @param strainCount strain count
     */
    public void init(int strainCount) throws Exception {

        genotypeCountMaps = new Object[strainCount];
        for( int i=0; i<strainCount; i++ ) {
            genotypeCountMaps[i] = new HashMap<String, Integer>();
        }
    }

    void processLine(String line, int strainCount, String[] header) throws Exception {

        String[] v = line.split("[\\t]", -1);

        if (v.length == 0 || v[0].length() == 0 || v[0].charAt(0) == '#')
            //skip lines with "#"
            return;

        // validate chromosome
        String chr = getChromosome(v[0]);
        // skip lines with invalid chromosomes (chromosome length must be 1 or 2
        if( chr==null || chr.length()>2 ) {
            return;
        }

        // variant pos
        int pos = Integer.parseInt(v[1]);

        String refNuc = v[3];
        String alleles = v[4];

        // get index of GQ - genotype quality
        String[] format = v[8].split(":");
        int ADindex = readADindex(format);
        int DPindex = readDPindex(format);
        if( ADindex < 0 || DPindex<0 ) {
            if( !processLinesWithMissingADDP ) {
                linesWithADorDPmissing++;
                return;
            }
        }

        // rgdid and hgvs name
        Integer rgdId = null;
        String hgvsName = null;
        String id = v[2];
        if( !Utils.isStringEmpty(id) && id.startsWith("RGDID:")) {
            // sample ID field for ClinVar:
            // RGDID:8650299;NM_001031836.2(KCNU1):c.2736+27C>T
            int semicolonPos = id.indexOf(';');
            if( semicolonPos>0 ) {
                rgdId = Integer.parseInt(id.substring(6, semicolonPos));
                hgvsName = id.substring(semicolonPos+1);
            } else {
                System.out.println("missing semicolon");
            }
        }

        for( int i=9; i<9+strainCount; i++ ) {
            String strain = header[i];
            processStrain(v[i], (HashMap<String, Integer>)genotypeCountMaps[i-9], ADindex, DPindex,
                    strain, chr, pos, refNuc, alleles, rgdId, hgvsName);
        }
    }

    void processStrain(String data, HashMap<String, Integer> genotypeCountMap, int ADindex, int DPindex, String strain,
                       String chr, int pos, String refNuc, String alleleString, Integer rgdId, String hgvsName) throws Exception {

        // skip rows with not present data (missing genotype)
        if( !handleGenotype(data.substring(0, 3), genotypeCountMap) )
            return;

        // read counts for all alleles, as determined by genotype
        int[] readCount = null;
        String[] arrValues = data.split(":"); // format is in 0/1:470,63:533:99:507,0,3909
        int readDepth = 0;
        if( ADindex>=0 ) {
            String[] value = arrValues[ADindex].split(",");
            if( value.length==1 && value[0].equals(".") ) {
                // special handling if hit counts are not known "." instead of expected "470,63" etc
                value = new String[]{"0","0","0","0","0","0","0"};
                linesWithADdotted++;
            }

            readCount = new int[value.length];
            for( int i=0; i<value.length; i++ ) {
                readDepth += readCount[i] = Integer.parseInt(value[i]);
            }
        } else {
            if( processLinesWithMissingADDP ) {
                readDepth = 9;
                readCount = new int[] {9, 9, 9, 9, 9, 9, 9, 9};
            }
        }

        int totalDepth = 0;
        if( DPindex>=0 ) {
            String dp = arrValues[DPindex];
            if( !dp.equals(".") ) {
                totalDepth = Integer.parseInt(dp);
                if( readDepth!=totalDepth ) {
                    //System.out.println("AD<>DP");
                }
            }
        } else {
            if( processLinesWithMissingADDP ) {
                totalDepth = 9;
            }
        }

        // for the single Reference to multiple Variants
        int alleleCount = getAlleleCount(alleleString);
        String[] alleles = (refNuc+","+alleleString).split(",");

        CommonFormat2Writer writer = getOutputFile(strain);

        // for every allele, including refNuc
        for (String allele: alleles ) {
            // skip the line variant if it is the same with reference (unless an override is specified)
            if( !processVariantsSameAsRef && refNuc.equals(allele) ) {
                continue;
            }

            CommonFormat2Line line = new CommonFormat2Line();
            line.setChr(chr);
            line.setPos(pos);
            line.setRefNuc(refNuc);
            line.setVarNuc(allele);
            line.setCountA(getReadCountForAllele("A", alleles, readCount));
            line.setCountC(getReadCountForAllele("C", alleles, readCount));
            line.setCountG(getReadCountForAllele("G", alleles, readCount));
            line.setCountT(getReadCountForAllele("T", alleles, readCount));
            if( totalDepth>0 )
                line.setTotalDepth(totalDepth);
            line.setAlleleDepth(getReadCountForAllele(allele, alleles, readCount));
            line.setAlleleCount(alleleCount);
            line.setReadDepth(readDepth);
            line.setRgdId(rgdId);
            line.setHgvsName(hgvsName);
            writer.writeLine(line);

            incrementVariantCount(strain, chr);
        }
    }

    int getReadCountForAllele(String allele, String[] alleles, int[] readCount) {

        for( int i=0; i<alleles.length; i++ ) {
            if( alleles[i].equals(allele) )
                return readCount[i];
        }
        return 0;
    }

    int getAlleleCount(String s) {
        int alleleCount = 1;
        for( int i=0; i<s.length(); i++ ) {
            if( s.charAt(i)==',' )
                alleleCount++;
        }
        return alleleCount;
    }

    int readADindex(String[] format) {

        // format : "GT:AD:DP:GQ:PL"
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("AD") ) {
                return i;
            }
        }

        // try CLCAD2
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("CLCAD2") ) {
                return i;
            }
        }
        return -1;
    }

    int readDPindex(String[] format) {

        // format : "GT:AD:DP:GQ:PL"
        // determine which position separated by ':' occupies
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("DP") ) {
                return i;
            }
        }
        return -1;
    }

    void dumpSummaries() {
        System.out.println("lines with AD or DP missing: "+linesWithADorDPmissing);
        System.out.println("lines with AD = '.'  : "+linesWithADdotted);

        // dump genotype count map
        //System.out.println("Genotype counts");
        //System.out.println("---------------");
        //for( Map.Entry<String, Integer> entry: genotypeCountMap.entrySet() ) {
        //    System.out.println("genotype "+entry.getKey()+": "+entry.getValue());
        //}
        System.out.println("Variant counts per sample and chromosome:");
        System.out.println("-----------------------------------------");
        for( Map.Entry<String,Integer> entry: variantCounts.entrySet() ) {
            System.out.println(entry.getKey()+":  "+entry.getValue());
        }
    }

    // return true if genotype is given, false if genotype is missing
    boolean handleGenotype(String genotype, Map<String,Integer> genotypeCountMap) {
        // skip rows with not present data
        Integer genotypeCount = genotypeCountMap.get(genotype);
        if( genotypeCount==null )
            genotypeCount = 1;
        else
            genotypeCount ++;
        genotypeCountMap.put(genotype, genotypeCount);

        if( genotype.equals("./.") )
            return false;
        if( genotype.equals("0/0") )
            return false;
        return true;
    }

    CommonFormat2Writer getOutputFile(String strain) throws IOException {
        CommonFormat2Writer writer = outputFiles.get(strain);
        if( writer==null ) {
            writer = new CommonFormat2Writer(compressOutputFile, appendToOutputFile);
            String fileName = outDir+"/"+strain.replace("/","_")+".txt";
            if( compressOutputFile ) {
                fileName += ".gz";
            }
            writer.create(fileName);
            outputFiles.put(strain, writer);
        }
        return writer;
    }

    void closeOutputFiles() throws Exception {
        for( CommonFormat2Writer writer: outputFiles.values() ) {
            writer.close();
        }
        CommonFormat2Writer.closeDbConnection();
    }

    void incrementVariantCount(String strain, String chr) {
        Integer count = variantCounts.get(strain);
        if( count==null )
            count = 1;
        else
            count++;
        variantCounts.put(strain, count);

        String key = strain + " - chr"+chr;
        count = variantCounts.get(key);
        if( count==null )
            count = 1;
        else
            count++;
        variantCounts.put(key, count);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
