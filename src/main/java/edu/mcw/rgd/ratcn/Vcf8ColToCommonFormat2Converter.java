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
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 9/12/14
 * Time: 8:38 AM
 * Converts VCF file with data for one strain in 8 columns format, into common format 2 file;
 * vcf file columns supported:
 *  0       1   2     3  4    5         6      7
 * #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
 * <p>
 * common format was 3 sets of tab delimited files as produced by early Illumina sequencers
 *   the drawback is that information is spread across 3 sets of files
 * common format 2 is a flat file format, designed by RGD
 *   it combines 3 sets of files used in common format 1 into one set of files
 */
public class Vcf8ColToCommonFormat2Converter extends VcfToCommonFormat2Base {

    List<String> vcfFiles = new ArrayList<>();
    String outFileName = null;
    String dbSnpSource = null;
    int mapKey = 0;
    boolean processVariantsSameAsRef = false;
    int badChrLines = 0;
    boolean compressOutputFile = false;
    boolean appendToOutputFile = false;

    CommonFormat2Writer outputFile = null;

    // variant count per chromosome
    Map<String, Integer> variantCounts = new TreeMap<>();
    private String version;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Vcf8ColToCommonFormat2Converter converter = (Vcf8ColToCommonFormat2Converter) (bf.getBean("vcf8col2commonFormat2"));

        converter.parseCmdLine(args);

        //String vcfFile = "/data/rat/indels/ACI_indels.vcf.gz";
        //String outFile = "/data/Hubrecht_rnor50";

        long timestamp = System.currentTimeMillis();
        converter.run();
        System.out.println("----- OK --------");
        System.out.println("Finished! Elapsed " + Utils.formatElapsedTime(timestamp, System.currentTimeMillis()));
    }

    void parseCmdLine(String[] args) throws Exception {

        // parse cmdline params
        for( int i=0; i<args.length; i++ ) {
            switch (args[i]) {
                case "--vcfFile":
                    vcfFiles.add(args[++i]);
                    break;
                case "--outFile":
                    outFileName = args[++i];
                    break;
                case "--mapKey":
                    mapKey = Integer.parseInt(args[++i]);
                    edu.mcw.rgd.datamodel.Map map = MapManager.getInstance().getMap(mapKey);
                    if (map != null) {
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
            }
        }

        System.out.println("Starting Vcf8ColToCommonFormat2Converter");
        for( String vcfFile: vcfFiles ) {
            System.out.println("  --vcfFile " + vcfFile);
        }
        System.out.println("  --outFile "+outFileName);
        System.out.println("  --mapKey "+mapKey);
        System.out.println("  --dbSnpSource  "+dbSnpSource);
        System.out.println("  --processVariantsSameAsRef  " + processVariantsSameAsRef);
        System.out.println("  --compressOutputFile  " + compressOutputFile);
        System.out.println("  --appendToOutputFile  " + appendToOutputFile);
        System.out.println();
    }

    public void run() throws Exception {

        for( String vcfFile: vcfFiles ) {
            System.out.println("Input file: "+vcfFile);
            run(vcfFile);
        }

        closeOutputFiles();

        dumpSummaries();
    }

    public void run(String vcfFile) throws Exception {

        BufferedReader reader = openInputFile(vcfFile);
        String line;

        // process all lines
        while( (line=reader.readLine())!=null ) {
            processLine(line);
        }
        reader.close();
    }

    void processLine(String line) throws Exception {

        String[] v = line.split("[\\t]", -1);

        if (v.length == 0 || v[0].length() == 0 || v[0].charAt(0) == '#') {
            //skip lines with "#"
            return;
        }

        // validate chromosome
        String chr = getChromosome(v[0]);
        if( chr==null ) {
            badChrLines++;
            return;
        }

        // variant pos
        int pos = Integer.parseInt(v[1]);

        String refNuc = v[3];
        String alleles = v[4];

        // build map of names and values from INFO field
        // f.e.: END=64603;DP=16;AC=9,7
        Map<String,String> data = new HashMap<>();
        for( String field: v[7].split("[\\;]",-1) ) {
            int posEQ = field.indexOf('=');
            if( posEQ>0 ) {
                data.put(field.substring(0, posEQ), field.substring(posEQ + 1));
            }
        }

        processStrain(data, chr, pos, refNuc, alleles);
    }

    void processStrain(Map<String,String> data, String chr, int pos, String refNuc, String alleleString) throws Exception {

        int totalDepth = Integer.parseInt(data.get("DP"));

        // f.e.: 'C,CT'  'A,G'
        String[] alleles = alleleString.split("[\\,]",-1);
        String[] alleleDepths = data.get("AC")==null ? null : data.get("AC").split("[\\,]",-1);

        // make sure that refNuc is always among the alleles
        boolean isRefNucAmongAlleles = false;
        for( int i=0; i<alleles.length; i++ ) {
            if( alleles[i].equals(refNuc) )  {
                isRefNucAmongAlleles = true;
                break;
            }
        }
        if( !isRefNucAmongAlleles && alleleDepths!=null ) {

            // compute depth for refnuc: totalDepth less depth for all alleles (that happen to NOT contain refNuc)
            int depthForAllelesExceptRefNuc = 0;
            for( int i=0; i<alleles.length; i++ ) {
                depthForAllelesExceptRefNuc += Integer.parseInt(alleleDepths[i]);
            }
            int depthForRefNuc = totalDepth - depthForAllelesExceptRefNuc;

            // append refNuc to alleles and refNucDepth to alleleDepths arrays, only if refNucDepth>0
            if( depthForRefNuc>0 ) {
                alleles = Arrays.copyOf(alleles, alleles.length+1);
                alleles[alleles.length-1] = refNuc;

                alleleDepths = Arrays.copyOf(alleleDepths, alleleDepths.length+1);
                alleleDepths[alleleDepths.length-1] = Integer.toString(depthForRefNuc);
            }
        }

        getOutputFile();

        // for every allele (varNuc)
        for( int i=0; i<alleles.length; i++ ) {
            String allele = alleles[i];

            // skip the line variant if it is the same with reference (unless an override is specified)
            if( !processVariantsSameAsRef && refNuc.equals(allele) ) {
                continue;
            }

            CommonFormat2Line line = new CommonFormat2Line();
            line.setChr(chr);
            line.setPos(pos);
            line.setRefNuc(refNuc);

            line.setVarNuc(allele);

            if( alleleDepths!=null ) {
                int alleleDepth = Integer.parseInt(alleleDepths[i]);

                line.setCountA(getSnpAlleleDepth("A", alleles, alleleDepths));
                line.setCountC(getSnpAlleleDepth("C", alleles, alleleDepths));
                line.setCountG(getSnpAlleleDepth("G", alleles, alleleDepths));
                line.setCountT(getSnpAlleleDepth("T", alleles, alleleDepths));

                line.setAlleleDepth(alleleDepth);
            } else {
                switch (allele) {
                    case "A":
                        line.setCountA(totalDepth);
                        break;
                    case "C":
                        line.setCountC(totalDepth);
                        break;
                    case "G":
                        line.setCountG(totalDepth);
                        break;
                    case "T":
                        line.setCountT(totalDepth);
                        break;
                }
                line.setAlleleDepth(totalDepth);
                line.setAlleleCount(1);
            }

            if( totalDepth>0 ) {
                line.setTotalDepth(totalDepth);
                line.setReadDepth(totalDepth);
            }

            outputFile.writeLine(line);

            incrementVariantCount(chr);
        }
    }

    int getSnpAlleleDepth(String allele, String[]alleles, String[]alleleDepths) {
        for( int i=0; i<alleles.length; i++ ) {
            if( alleles[i].equals(allele) )
                return Integer.parseInt(alleleDepths[i]);
        }
        return 0;
    }

    BufferedReader openInputFile(String file) throws IOException {
        if( file.endsWith(".gz") ) {
            return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        }
        else {
            return new BufferedReader(new FileReader(file));
        }
    }

    void dumpSummaries() {
        System.out.println("Lines with bad chromosome: "+badChrLines);
        System.out.println("Variant counts per sample and chromosome:");
        System.out.println("-----------------------------------------");
        int total = 0;
        for( Map.Entry<String,Integer> entry: variantCounts.entrySet() ) {
            System.out.println(entry.getKey()+":  "+entry.getValue());
            total += entry.getValue();
        }
        System.out.println("TOTAL:  "+total);
    }

    void getOutputFile() throws IOException {
        if( outputFile==null ) {
            outputFile = new CommonFormat2Writer(compressOutputFile, appendToOutputFile);
            outputFile.create(outFileName);
        }
    }

    void closeOutputFiles() throws Exception {
        outputFile.close();
        outputFile.closeDbConnection();
    }

    void incrementVariantCount(String chr) {
        Integer count = variantCounts.get(chr);
        if( count==null )
            count = 1;
        else
            count++;
        variantCounts.put(chr, count);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
