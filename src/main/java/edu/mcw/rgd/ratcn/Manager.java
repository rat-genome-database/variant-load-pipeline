package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.ratcn.convert.ClinVar2Vcf;
import edu.mcw.rgd.ratcn.convert.DbSnp2Vcf;
import edu.mcw.rgd.ratcn.convert.GwasCat2Vcf;
import edu.mcw.rgd.ratcn.fixup.*;

/**
 * @author mtutaj
 * @since 11/13/13
 * wrapper class to run tools available in the suite
 */
public class Manager {

    public static void main(String[] args) throws Exception {

        // first parameter is the name of tool
        if( args.length<=2 || !args[0].equals("--tool") ) {
            printUsage();
            return;
        }
        String tool = args[1];

        // make copy of remaining parameters
        String[] toolArgs = new String[args.length - 2];
        System.arraycopy(args, 2, toolArgs, 0, args.length - 2);

        // run tool in question
        try {
            switch (tool) {
                case "VariantRatLoaderFromDb":
                    VariantRatLoaderFromDb.main(toolArgs);
                    break;
                case "VcfConverter2":
                    VcfToCommonFormat2Converter.main(toolArgs);
                    break;
                case "Vcf8ColConverter2":
                    Vcf8ColToCommonFormat2Converter.main(toolArgs);
                    break;
                case "VariantLoad3":
                    VariantLoad3.main(toolArgs);
                    break;
                case "VariantPostProcessing":
                    VariantPostProcessing.main(toolArgs);
                    break;
                case "Polyphen":
                    Polyphen.main(toolArgs);
                    break;
                case "Polyphen2":
                    Polyphen2.main(toolArgs);
                    break;
                case "PolyphenFasta":
                    PolyphenFasta.main(toolArgs);
                    break;
                case "PolyphenLoader":
                    PolyphenLoader.main(toolArgs);
                    break;
                case "PolyphenLoader2":
                    PolyphenLoader2.main(toolArgs);
                    break;
                case "VariantTypeFixUp":
                    VariantTypeFixUp.main(toolArgs);
                    break;
                case "FrameShiftFixUp":
                    FrameShiftFixUp.main(toolArgs);
                    break;
                case "GenicStatusFixUp":
                    GenicStatusFixUp.main(toolArgs);
                    break;
                case "ClinVar2Vcf":
                    ClinVar2Vcf.main(toolArgs);
                    break;
                case "DbSnp2Vcf":
                    DbSnp2Vcf.main(toolArgs);
                    break;
                case "ConservationScore":
                    ConservationScoreLoader.main(toolArgs);
                    break;
                case "GwasCat2Vcf":
                    GwasCat2Vcf.main(toolArgs);
                    break;
                default:
                    printUsage();
            }
        }catch(Exception e) {
            System.out.println();
            System.out.println("ERROR: PIPELINE ABORTED!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    static void printUsage() {

        System.out.println("RatStrainLoader Tool Suite usage:");
        System.out.println();
        System.out.println(" --tool [VcfConverter2|Vcf8ColConverter2|VariantLoad3|VariantPostProcessing|Polyphen|PolyphenFasta|PolyphenLoader|ClinVar2Vcf|ConservationScore] <tool-dependent-parameters>");
        System.out.println();
        System.out.println(" VcfConverter2 params");
        System.out.println("   --vcfFile <path to VCF file>");
        System.out.println("   --outDir <path to output dir where files in common format will be created for every sample >");
        System.out.println("   --mapKey <optional; MAP_KEY to be used when retrieving data from DB_SNP table; default is 70>");
        System.out.println("   --processVariantsSameAsRef <optional; process variants even if they are the same as the reference (by default they are skipped)>");
        System.out.println("   --compressOutputFile <optional; gzip the output file on the fly>");
        System.out.println("   --appendToOutputFile <optional; append the output to the existing file if any>");
        System.out.println();
        System.out.println(" Vcf8ColConverter2 params (convert vcf4.0 file into common format ver2");
        System.out.println("   vcf file contains only 8 columns, for one strain: CHR-POS-ID-REF-ALT-QUAL-FILTER-INFO");
        System.out.println("   --vcfFile <path to VCF file(s); multiple --vcfFile parameters are supported>");
        System.out.println("   --outFile <path to output file>");
        System.out.println("   --mapKey <optional; MAP_KEY to be used when retrieving data from DB_SNP table; default is 70>");
        System.out.println("   --processVariantsSameAsRef <optional; process variants even if they are the same as the reference (by default they are skipped)>");
        System.out.println("   --appendToOutputFile <optional; append the output to the existing file if any>");
        System.out.println();
        System.out.println(" VariantLoad3 params");
        System.out.println("   --sampleId <sample id>");
        System.out.println("     -s");
        System.out.println("   --inputFile <path to sample input file in common format version 2>");
        System.out.println("     -i");
        System.out.println("   --logFileName <by default, it is 'variantIlluminaLoad'>");
        System.out.println("     -l");
        System.out.println("   --verifyIfInRgd <if specified, variant is inserted only if it is not in RGD>");
        System.out.println("     -v");
        System.out.println("   Note: you can specify multiple pairs of -s -i parameters");
        System.out.println("         to load multiple samples at the same time.");
        System.out.println();
        System.out.println(" VariantPostProcessing params");
        System.out.println("   --sampleId <sample-id> -- sample id; you can specify multiple samples");
        System.out.println("   --fastaDir <fasta-dir> -- directory with reference .fa.gz files");
        System.out.println("   --verifyIfInRgd   -- check if given row is already in RGD");
        System.out.println("   --chr <chromosome> -- optional, if not given, all chromosomes are processed");
        System.out.println();
        System.out.println(" Polyphen params");
        System.out.println("   --sample <sample-id>");
        System.out.println("   --chr <chromosome> -- optional, if not given, all chromosomes are processed");
        System.out.println("   --outDir <outDir> -- optional, default is /data/rat/output");
        System.out.println();
        System.out.println(" PolyphenFasta params");
        System.out.println("   --sample <sample-id>");
        System.out.println("   --chr <chromosome> -- optional, if not given, all chromosomes are processed");
        System.out.println("   --resultsDir <inDir> -- optional, default is /data/rat/results");
        System.out.println("   --outputDir <inDir> -- optional, default is /data/rat/output");
        System.out.println();
        System.out.println(" PolyphenLoader params");
        System.out.println("   --sample <sample-id>");
        System.out.println("   --chr <chromosome> -- optional, if not given, all chromosomes are processed");
        System.out.println("   --resultsDir <inDir> -- optional, default is /data/rat/results");
        System.out.println("   --outputDir <inDir> -- optional, default is /data/rat/output");
        System.out.println();
        System.out.println(" ClinVar2Vcf params -- multiple (mapKey,outputFile) pairs are possible");
        System.out.println("   --mapKey <mapKey>");
        System.out.println("   --outputFile <outputFile>");
        System.out.println();
        System.out.println(" DbSnp2Vcf params -- multiple (mapKey,source,outputFile)  are possible");
        System.out.println("   --mapKey <mapKey>");
        System.out.println("   --source <f.e. dbSnp150>");
        System.out.println("   --outputFile <outputFile>");
        System.out.println();
        System.out.println(" ConservationScore");
        System.out.println("   --fileName <name of gzipped file with phastCons scores in wig format>");
        System.out.println("   --tableName <name of table the data is to be put into>");
    }
}
