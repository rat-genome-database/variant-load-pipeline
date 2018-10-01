package edu.mcw.rgd.ratcn.convert;

import java.util.HashMap;

/**
 * Format:
 [0]     [1]     [2]     [3]     [4]     [5]     [6]     [7]     [8]     [9]     [10]    [11]    [12]    [13]
#CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO    FORMAT  ACI     FHH     FHL     SR      SS
chr1    4095    .       G       T       193.34  PASS    AC=6;AF=1.00;AN=6;DP=15;Dels=0.00;FS=0.000;HRun=1;HaplotypeScore=0.0000;
MQ=24.60;MQ0=3;QD=13.81 GT:AD:DP:GQ:PL  ./.     1/1:0,7:7:15.04:167,15,0        1/1:0,1:1:3.01:33,3,0   1/1:0,6:6:6.01:59,6,0
 */

public class RatMcwVCF extends AbstractVariantDataMapper {

    private boolean processVariantsSameAsReference;

    // for every strain we have one genotype count map
    Object[] genotypeCountMaps;
    public String[] header; // VCF header line, containing strain names starting at index 9

    // how many lines have AD:DP missing?
    int linesWithADmissing = 0;
    // how many lines have value of AD = "."
    int linesWithADdotted = 0;

    public boolean isProcessVariantsSameAsReference() {
        return processVariantsSameAsReference;
    }

    public void setProcessVariantsSameAsReference(boolean processVariantsSameAsReference) {
        this.processVariantsSameAsReference = processVariantsSameAsReference;
    }

    public void initGenotypeCountMaps(int strainCount) {

        genotypeCountMaps = new Object[strainCount];
        for( int i=0; i<strainCount; i++ ) {
            genotypeCountMaps[i] = new HashMap<String, Integer>();
        }
    }

    public void processLine(String line, int strainCount) throws Exception {

        String[] v = line.split("\t");

        if (v == null || v.length == 0 || v[0].length() == 0 || v[0].charAt(0) == '#')
            //skip lines with "#"
            return;

        // get index of GQ - genotype quality
        String format = v[8];
        int ADindex = readADindex(format);
        if( ADindex < 0 ) {
            linesWithADmissing++;
        }

        for( int i=9; i<9+strainCount; i++ ) {
            processStrain(v, i, (HashMap<String, Integer>)genotypeCountMaps[i-9], ADindex);
        }
    }

    void processStrain(String[] v, int dataCol, HashMap<String, Integer> genotypeCountMap, int ADindex) throws Exception {

        if( dataCol>=v.length ) {
            System.out.println("array index issue");
        }
        String data = v[dataCol];

        // skip rows with not present data
        String genotype = data.substring(0, 3);
        Integer genotypeCount = genotypeCountMap.get(genotype);
        if( genotypeCount==null )
            genotypeCount = 1;
        else
            genotypeCount ++;
        genotypeCountMap.put(genotype, genotypeCount);

        if( genotype.equals("./.") )
            return;
        if( genotype.equals("0/0") ) {
            return;
        }

        // read counts for all alleles, as determined by genotype
        int[] readCount = null;
        int readCountIndex = 1; // at index 0 there is refCount
        // readDepth, Calculated by counting the occurrences of the variant in the sequence (Read bases at a SNP line ) - column 9
        int readDepth=0, varRead;
        String[] arrValues = data.split(":"); // format is in 0/1:470,63:533:99:507,0,3909
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
        }

        // for the single Reference to multiple Variants
        String[] varsArray = v[4].split(",");
        int varsNumber = varsArray.length;

        for (int i = 0; i < varsNumber; i++) {
            String thisVariant = varsArray[i];

            // skip the line variant if it is the same with reference (unless an override is specified)
            if( !isProcessVariantsSameAsReference() && v[3].equals(thisVariant) ) {
                continue;
            }

            // skip variants where number of reads is 0
            if( readCountIndex>=readCount.length ) {
                System.out.println("unexpected");
            }
            varRead = readCount[readCountIndex++];
            if( varRead==0 ) {
                continue;
            }

            VariantRecord vr = new VariantRecord();

            String chromosomeStr = v[0].replaceAll("chr", "");
            vr.setChromosome(chromosomeStr);
            // skip lines with invalid chromosomes (chromosome length must be 1 or 2
            if( chromosomeStr.length()>2 ) {
                continue;
            }

            vr.setReadDepth(readDepth);

            // score: Zygosity Percent = Variant Read depth/total depth(calculated above) * 100
            double score = (float) varRead / readDepth * 100;
            if (score > 0 && score < 99) {
                score = score + 0.5;
            }
            //vr.setZygosityPercent((int)score);

            vr.setStart(Long.parseLong(v[1]));
            //vr.setStop(Long.parseLong(v[1]));
            vr.setReferenceNucleotide(v[3]);
            vr.setVariantNucleotide(thisVariant); // v[4]
            vr.setQualityScore(Integer.toString((int) score));
            vr.setVarName(v[2]);

            this.write(header[dataCol], vr);


            HashMap hmReads = new HashMap();
            hmReads.put(v[3], readCount[0]);
            hmReads.put(thisVariant, varRead);

            // record the Zygosity and Depth
            VariantReads vreads = new VariantReads();
            vreads.setPosition(v[1]);
            vreads.setChromosome(chromosomeStr);
            if (hmReads.get("A") != null) {
                vreads.setReadCountA(hmReads.get("A").toString());
            } else {
                vreads.setReadCountA("0");
            }
            if (hmReads.get("T") != null) {
                vreads.setReadCountT(hmReads.get("T").toString());
            } else {
                vreads.setReadCountT("0");
            }
            if (hmReads.get("C") != null) {
                vreads.setReadCountC(hmReads.get("C").toString());
            } else {
                vreads.setReadCountC("0");
            }
            if (hmReads.get("G") != null) {
                vreads.setReadCountG(hmReads.get("G").toString());
            } else {
                vreads.setReadCountG("0");
            }

            vreads.setRefNuc(v[3]);
            vreads.setModifiedCall(thisVariant);

            this.writeVarRead(header[dataCol], vreads);
        }
    }

    int readADindex(String format) {

        // format : "GT:AD:DP:GQ:PL"
        // determine which position separated by ':' occupies
        int i = format.indexOf("AD");
        if( i<0 )
            return i;
        return i/3;
    }

    public void dumpSummaries() {
        System.out.println("lines with AD missing: "+linesWithADmissing);
        System.out.println("lines with AD = '.'  : "+linesWithADdotted);

        // dump genotype count map
        System.out.println("Genotype counts");
        System.out.println("---------------");
        //for( Map.Entry<String, Integer> entry: genotypeCountMap.entrySet() ) {
        //    System.out.println("genotype "+entry.getKey()+": "+entry.getValue());
        //}

    }
}