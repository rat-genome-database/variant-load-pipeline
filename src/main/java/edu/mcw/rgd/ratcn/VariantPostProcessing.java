package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * @author mtutaj
 * @since 8/8/13
 * Replaces original variant post processing code from carpenovo
 */
public class VariantPostProcessing extends VariantProcessingBase {

    private String version;
    private String logFile;
    private String fastaDir;
    private String logDir;
    private boolean verifyIfInRgd = false;
    int mapKey = 0;
    private GeneCache geneCache = new GeneCache();
    private TranscriptCache transcriptCache = new TranscriptCache();
    private TranscriptFeatureCache transcriptFeatureCache = new TranscriptFeatureCache();
    MapDAO mdao = new MapDAO();
    TranscriptDAO tdao = new TranscriptDAO();

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        VariantPostProcessing instance = (VariantPostProcessing) (bf.getBean("variantPostProcessing"));

        // process args
        List<Integer> mapKeys = new ArrayList<>();
        String chr = null;

        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--mapKey") ) {
                mapKeys.add(Integer.parseInt(args[++i]));
            }

            if( args[i].equals("--fastaDir") ) {
                instance.setFastaDir(args[++i]);
                System.out.println("FASTA_DIR " + instance.getFastaDir());
            }

            if( args[i].equals("--verifyIfInRgd") ) {
                instance.verifyIfInRgd = true;
            }

            if( args[i].equals("--chr") ) {
                chr = args[++i];
                System.out.println("CHR = "+chr);
            }
        }

        System.out.println("VERIFY_IF_IN_RGD = "+instance.verifyIfInRgd);

        for( Integer key: mapKeys ) {
            instance.mapKey = key;
            instance.run( chr);
        }
    }

    public VariantPostProcessing() throws Exception {

    }

    void run( String chr) throws Exception {

        String msg =
                getVersion() + "\n" +
                        "processing assembly " + mapKey + "\n" +
                        "   chromosome: " + chr + "\n";
        System.out.println(msg);
        logStatusMsg(msg);

        // open main log file
        String fileName = getLogDir()+getLogFile();
        fileName = fileName.replace("###ASSEMBLY###", Integer.toString(mapKey));
        setLogWriter(new BufferedWriter(new FileWriter(fileName)));
        getLogWriter().write(msg);

        // Log start
        insertSystemLogMessage("variantPostProcessing", "Started for Assembly " + mapKey);

        try {
            prepareStatements();
            processChromosomes( chr);
            closePreparedStatements();
        }
        catch(Exception e) {
            e.printStackTrace();

            getLogWriter().flush();

            insertSystemLogMessage("variantPostProcessing", "Broken for Assembly "+mapKey);

            throw e;
        }

        // Log end
        insertSystemLogMessage("variantPostProcessing", "Updated for Assembly "+mapKey);

        msg = "Assembly "+mapKey+" processing complete!";
        System.out.println(msg);
        logStatusMsg(msg);
        getLogWriter().write("Assembly processing complete!\n");
        getLogWriter().close();

        System.out.println();
        getDataSource().getConnection().close();
    }

    void processChromosomes( String chrOverride) throws Exception {

        System.out.println("process chromosomes for mapKey "+mapKey );
        //ChrFastaFile fastaFile = new ChrFastaFile();
        FastaParser fastaParser = new FastaParser();
        fastaParser.setMapKey(mapKey, this.getFastaDir());
        List<String> chromosomes = getChromosomes(mapKey);
        Collections.shuffle(chromosomes); // randomize chromosomes (works better during simultaneous processing of multiple samples)
        for( String chr: chromosomes ) {
            if( chrOverride!=null && !chrOverride.equals(chr) ) {
                continue;
            }

            System.out.println("  chr "+chr);

            processChromosome(chr, fastaParser);

            getLogWriter().flush();
        }
        System.out.println("---OK---");
    }

    ///////////////////////////////////////////////////////////////////////////
    // Process each Chromosome file by reading it into memory , selecting the variants
    // for this chromosome and processing these variants
    //
    private VariantTranscriptBatch batch;
    void processChromosome(String chr, FastaParser fastaFile) throws Exception {

        long timestamp = System.currentTimeMillis();
        logStatusMsg("CHR " + chr);
        getLogWriter().write("----------------- Start Processing of Chromosome " + chr + " -----------------\n");
        fastaFile.setChr(chr);

        long totalCount = 1;
        batch = new VariantTranscriptBatch();
        batch.setVerifyIfInRgd(verifyIfInRgd);

        int preloadedCount;
        if(verifyIfInRgd) {
            getLogWriter().write("------ PRELOAD VARIANT_TRANSCRIPT for chr"+chr+" --------\n");
            preloadedCount = batch.preloadVariantTranscriptData(mapKey, chr);
            logStatusMsg("------ PRELOADED: " + preloadedCount + "\n");
            System.out.println("-- VT CACHE PRELOADED: " + preloadedCount);
        }
        getLogWriter().write("------ INIT GENE CACHE for chr"+chr+" --------\n");
        preloadedCount = geneCache.loadCache(mapKey, chr, getDataSource());
        logStatusMsg("------ INIT GENE CACHE for chr"+chr+" complete: "+preloadedCount+"\n");
        System.out.println("-- GENE CACHE PRELOADED: "+preloadedCount);

        getLogWriter().write("------ INIT TRANSCRIPT CACHE for chr"+chr+" --------\n");
        preloadedCount = transcriptCache.loadCache(mapKey, chr, getDataSource());
        logStatusMsg("------ INIT TRANSCRIPT CACHE for chr"+chr+" complete: "+preloadedCount+"\n");
        System.out.println("-- TRANSCRIPT CACHE PRELOADED: "+preloadedCount);

        getLogWriter().write("------ INIT TRANSCRIPT FEATURE CACHE for chr"+chr+" --------\n");
        preloadedCount = transcriptFeatureCache.loadCache(mapKey, chr, getDataSource());
        logStatusMsg("------ INIT TRANSCRIPT FEATURE CACHE for chr"+chr+" complete: "+preloadedCount+"\n");
        System.out.println("-- TRANSCRIPT FEATURE CACHE PRELOADED: "+preloadedCount);


        // Iterate over all the variants for the given sample_id and chr
        //
        ResultSet variantRow = getVariantResultSet(mapKey, chr);

        while( variantRow.next() ) {
            long variantId = variantRow.getLong(1);
            int varStart = variantRow.getInt(2);
            int varStop = variantRow.getInt(3);
            String variantNuc = variantRow.getString(4);
            String refNuc = variantRow.getString(5);

            getLogWriter().write("------------------- Start Processing of Variant ---------\n");
            getLogWriter().write("Processing variant id " + variantId + " Variant count : " + totalCount + "\n");

            // Get all GENES for this variant
            for( int geneRgdId: geneCache.getGeneRgdIds(varStart) ) {
                getLogWriter().write("	--------------- Start Processing for gene rgdId " + geneRgdId + " --------\n");
                initGene(geneRgdId);

                // Iterate over all transcripts for this gene
               // ResultSet rgdRow = getTranscriptsResultSet(geneRgdId, sample.getMapKey());
              //  while( rgdRow.next() ) {
                List<TranscriptCache.TranscriptCacheEntry> entries = transcriptCache.getTranscripts(geneRgdId);
                if(entries != null) {
                    for (TranscriptCache.TranscriptCacheEntry entry : entries) {
                        //    int transcriptRgdId = rgdRow.getInt(1);

                        getLogWriter().write("	------------- Start Processing of Transcript " + entry.transcriptRgdId + " ---\n");


                        // Get count of exons as we need to ignore the last position of the last exon
                        int totalExonCount = getExonCount(entry.transcriptRgdId, chr, mapKey);
                        getLogWriter().write("				totalExonCount : " + totalExonCount + "\n");

                        TranscriptFlags tflags = new TranscriptFlags();


                        // See if we have a Coding region
                        //   String isNonCodingRegion = rgdRow.getString(2);


                        processFeatures(entry.transcriptRgdId, chr, mapKey, tflags, varStart, varStop, totalExonCount);

                        // not found means it was in an INTRON Region
                        if (!tflags.inExon) {
                            if (tflags.transcriptLocation != null) {
                                tflags.transcriptLocation += ",INTRON";
                            } else {
                                tflags.transcriptLocation = "INTRON";
                            }
                        }
                        getLogWriter().write("transcriptLocation" + tflags.transcriptLocation + "\n");

                        // If not in Exome log and continue
                        boolean doInsert = false;
                        if (!tflags.inExon || entry.isNonCodingRegion.equals("Y")) {
                            if (entry.isNonCodingRegion.equals("Y")) {
                                if (tflags.transcriptLocation != null) {
                                    tflags.transcriptLocation += ",NON-CODING";
                                } else {
                                    tflags.transcriptLocation = "NON-CODING";
                                }
                            }
                            doInsert = true;
                        } else {
                            boolean wasInserted = processTranscript(tflags, entry.transcriptRgdId, fastaFile,
                                    varStart, varStop, variantId, refNuc, variantNuc, mapKey,chr);
                            if (!wasInserted) {
                                doInsert = true;
                            }
                        }

                        if (doInsert) {
                            insertVariantTranscript(variantId, entry.transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite);
                            // No need to determine Amino Acids if variant not in coding part of exon
                        }
                    }
                }
               // rgdRow.close();
            }
            totalCount += 1;
        }
        variantRow.close();

        batch.flush();

        String msg = "assembly="+mapKey+" chr"+chr+"  VARIANT_TRANSCRIPT rows inserted=" + batch.getRowsCommitted()
                +", up-to-date="+batch.getRowsUpToDate()
                +", time elapsed " + Utils.formatElapsedTime(timestamp, System.currentTimeMillis());
        System.out.println(msg);
        logStatusMsg(msg);
        logStatusMsg("Total variants for assembly="+mapKey+" chr"+chr+" = "+totalCount);
    }

    void processFeatures(int transcriptRgdId, String chr, int mapKey, TranscriptFlags tflags, int varStart, int varStop, int totalExonCount) throws Exception {

        // Get all Transcript features for this transcript. These are Exoms, 3primeUTRs and 5PrimeUTRs
   //     ResultSet transRow = getTranscriptFeaturesResultSet(transcriptRgdId, chr, mapKey);
   //     while( transRow.next() ) {
        List<TranscriptFeatureCache.TranscriptFeatureCacheEntry> rows = transcriptFeatureCache.getTranscriptFeatures(transcriptRgdId);
        if(rows != null) {
            for (TranscriptFeatureCache.TranscriptFeatureCacheEntry transRow : rows) {

                // Assume all rows have the same strand
                tflags.strand = transRow.strand;
                int transStart = transRow.startPos;
                int transStop = transRow.stopPos;
                String objectName = transRow.objectName;
                getLogWriter().write("		Found: " + objectName + " " + transStart + " - " + transStop + " (" + tflags.strand + ") \n");

                if (objectName.equals("3UTRS")) {
                    tflags.threeUtr = new Feature(transStart, transStop, objectName);
                }
                if (objectName.equals("5UTRS")) {
                    tflags.fiveUtr = new Feature(transStart, transStop, objectName);
                }
                if (objectName.equals("EXONS")) {
                    tflags.exomsArray.add(new Feature(transStart, transStop, objectName));

                // 1. Determine splice site is wihtin 10 BP of start / stop of any EXON
                // 2. Check the start position unless it is the start of the first EXON
                if (tflags.exomsArray.size() != 1) {
                    // If the transcript start falls within 10 bp of the variant
                    if ((transStart - 10 <= varStart) && (transStart + 10 >= varStop)) {
                        getLogWriter().write("nearSpliceSite found for : transcriptRGDId : " + transcriptRgdId + " , variantStart: " + varStart + ",  transStart: ${transStart} , threeUtr.start: ${threeUtr?.start} threeUtr.stop: ${threeUtr?.stop} fiveUtr.start : ${fiveUtr?.start}  fiveUtr.stop : ${fiveUtr?.stop}\n");
                        tflags.nearSpliceSite = "T";
                    }
                }

                // 3. Check the stop position unless it is the stop position of  last exon
                if (tflags.exomsArray.size() != totalExonCount) {
                    // If the transcript stop falls within 10 bp of the variant stop
                    if ((transStop - 10 <= varStart) && (transStop + 10 >= varStop)) {
                        getLogWriter().write("nearSpliceSite found for : transcriptRGDId : " + transcriptRgdId + " ,variantStart: " + varStart + " ,  transStart: " + transStart + " , threeUtr.start: ${threeUtr?.start} threeUtr.stop: ${threeUtr?.stop} fiveUtr.start : ${fiveUtr?.start}  fiveUtr.stop : ${fiveUtr?.stop}\n");
                        tflags.nearSpliceSite = "T";
                    }
                }
            }

            // See if our variant falls into this particular feature , we grad the first feature as the 3Prime and 5prime come first
            // and skip the EXONS as they would also match which we don't want
            //

            // Determine up the transcipt Location  , we want one of these strings:
            // "3UTR,EXON" or "3UTR,INTRON" or "EXON" or "INTRON" or "5UTR,EXON" or "5UTR,INTRON"

            if( transStart <= varStart && transStop >= varStop ) {
                getLogWriter().write("	Object found " + objectName + "\n");

                if ((objectName.equals("5UTRS")) || (objectName.equals("3UTRS"))) {
                    if (tflags.transcriptLocation != null) {
                        tflags.transcriptLocation += "," + objectName;
                    } else {
                        tflags.transcriptLocation = objectName;
                    }
                }
                // Add only one EXON using inExon to not do this again
                if (objectName.equals("EXONS") && (!tflags.inExon)) {
                    if (tflags.transcriptLocation != null) {
                        tflags.transcriptLocation += ",EXON";
                    } else {
                        tflags.transcriptLocation = "EXON";
                    }
                    tflags.inExon = true;
                }
                getLogWriter().write("transcriptLocation" + tflags.transcriptLocation + "\n");
            }
        }
        }
      //  transRow.close();
    }

    // Process the variant exoms and UTRS  creating the variant_transcript.
    // Called only if the variant falls in the exons
    // if a variant falls into exon utr area, it is not processed, and false is returned
    // otherwise true is returned
    boolean processTranscript(TranscriptFlags tflags, int transcriptRgdId, FastaParser fastaFile,
                              int varStart, int varStop, long variantId, String refNuc, String varNuc, int mapKey,String chr) throws Exception {

        if (tflags.strand != null && tflags.strand.equals("-") ) {
            getLogWriter().write("Switching UTrs as we're dealing with - strand ... \n");
            Feature temp = tflags.threeUtr;
            tflags.threeUtr = tflags.fiveUtr;
            tflags.fiveUtr = temp;
        }

        // OK we' have a variant in an Exom  for only plus stranded genes !!!!!!
        handleUTRs(tflags.exomsArray, tflags.threeUtr, tflags.fiveUtr);

        getLogWriter().write("		        Variant : " + varStart + " " + varStop + "\n");
        getLogWriter().write(" 		Processed exons :\n");
        int fcount = 1;
        int variantRelPos = 0; // relative position of the variant in the entire combined exome sequence
        boolean foundInExon = false;
        // Determine the relative position the variant occurs at
        for (Feature feature: tflags.exomsArray) {
            getLogWriter().write(" 		EXON start :" + feature.start + " stop " + feature.stop + "\n");
            // See if feature was skipped entirely by removal from 5PrimteUTR
            if (feature.start != -1) {
                if (feature.start <= varStart && feature.stop > varStop) {
                    getLogWriter().write(" 		DNA :" + getDnaChunk(fastaFile, feature.start, feature.stop) + "\n");
                    foundInExon = true;
                    getLogWriter().write("Variant found in feature # " + fcount + "\n");
                    variantRelPos += (varStart - (feature.start - 1)); // add length of partial feature
                    getLogWriter().write("Relative variant position found as " + variantRelPos + "\n");
                    break;
                } else {
                    variantRelPos += (feature.stop - feature.start) + 1;  // add length of entire feature
                }
            }
            fcount++;
        }
        getLogWriter().write("			variantRelPos = " + variantRelPos + "\n");
        if (foundInExon) {
            getLogWriter().write("************************* Variant in Exome region ************************\n");
            StringBuffer refDna = new StringBuffer();
            StringBuffer varDna = new StringBuffer();

            // Build up DNA Sequence from features
            for (Feature feature: tflags.exomsArray) {
                // Skip those exons that have been removed. These have had their start / stop marked as -1
                if (feature.start != -1) {
//                    String dnaChunk = getDnaChunk(fastaFile, feature.start, feature.stop);
                    String dnaChunk = getProperChunk(fastaFile, transcriptRgdId, chr, feature.start, feature.stop, mapKey);
                    getLogWriter().write("Building dna adding : (" + feature.start + ", " + feature.stop + ") " + dnaChunk + " length : " + dnaChunk.length() + "\n");
                    refDna.append(dnaChunk);
                    varDna.append(dnaChunk);
                }
            }
            varDna = new StringBuffer(varDna.toString().toLowerCase());

            // handle deletion
            if( varNuc == null || varNuc.contains("-")  ) {
                int deletionLength;
                if(varNuc == null)
                    deletionLength = 1;
                else deletionLength = varNuc.length();
                varDna.replace(variantRelPos-1, variantRelPos-1+deletionLength, "");
            }
            // handle insertion
            else if(refNuc == null || refNuc.contains("-") ) {
                varDna.insert(variantRelPos-1, varNuc);
            }
            // handle insertion
            else if( refNuc.length()==1 && varNuc.length()>1 ) {
                varDna.insert(variantRelPos, varNuc.substring(1));
            }
            else if( refNuc.length()!=1 || varNuc.length()!=1 ) {
                int deletionLength = varStop-varStart;
                varDna.replace(variantRelPos-1, variantRelPos-1+deletionLength, varNuc);
            }
            else {
                varDna.setCharAt(variantRelPos-1, varNuc.charAt(0));
            }

            refDna = new StringBuffer(refDna.toString().toLowerCase());

            getLogWriter().write(" RefDna length =  : " + refDna.length() + " mod " + (refDna.length() % 3) + "\n");
            getLogWriter().write(" ENTIRE reference DNA :\n" + refDna.toString() + "\n");


            if (tflags.strand.equals("-")) {
                getLogWriter().write("		Negative Strand found reverseCompliment dna \n");
                getLogWriter().write("		variantRelPos before neg stand switch is : " + variantRelPos + "\n");
                variantRelPos = refDna.length() - variantRelPos + 1;
                getLogWriter().write("		variantRelPos set now set to : " + variantRelPos + "\n");
                // Dealing with "-" strand , reverse the DNA
                refDna = reverseComplement(refDna, transcriptRgdId);
                varDna = reverseComplement(varDna, transcriptRgdId);
            } else {
                getLogWriter().write("		Positive Strand found " + "\n");
            }

            // Check for rna evenly divisable by 3 or log as error
            String transcriptErrorFound = "F";
            if (refDna.length() % 3 != 0) {
                writeError(variantId+":"+transcriptRgdId+":"+refDna.length()+":"+((new Date()).toString())+":TRIPLETERROR\n", mapKey);
                getLogWriter().write("************************* Warning in transcript rna length : see error_rga.txt file  ************************\n");
                // Use this later to update VARIANT+TRANSCRIPT table with error
                transcriptErrorFound = "T";
            }
            // make it divisible by 3
            if (refDna.length() % 3 != 0) {
                refDna.replace(0, refDna.length(), refDna.substring(0, refDna.length() - (refDna.length() % 3)));
                getLogWriter().write(" RefDna fixed div 3 length =  : " + refDna.length() + " mod " + (refDna.length() % 3) + "\n");
            }
            if (varDna.length() % 3 != 0) {
                varDna.replace(0, varDna.length(), varDna.substring(0, varDna.length() - (varDna.length() % 3)));
                getLogWriter().write(" VarDna fixed div 3 length =  : " + varDna.length() + " mod " + (varDna.length() % 3) + "\n");
            }

            // Now test to see if the variant was in an area eliminated by the divisable by 3 truncation process
            if ( variantRelPos < 1 ) {
                writeError(variantId+":"+transcriptRgdId+":"+refDna.length()+":"+new Date().toString()+":SKIPPED\n", mapKey);
                getLogWriter().write("************************* Error in transcript variant in trimmed area : skipping see error_rga.txt file  ************************\n");
                return false; // return false to insert new row into VARIANT_TRANSCRIPT: at least variant location will be available

            }


            getLogWriter().write("ENTIRE Ref DNA :\n" + refDna.toString() + "\n");

            // replace variant , ok as the variant is always on the plus strand
            getLogWriter().write("variant rel pos = " + variantRelPos + "\n");

            getLogWriter().write(" ENTIRE variant DNA ( change in upper case ):\n" + varDna.toString() + "\n");


            return handleTranslatedProtein(refDna, varDna, variantRelPos, variantId,
                    transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite, transcriptErrorFound,chr);
        } else {
            //variant lies within an exon but the part of the exon where it lies is not protein coding
            // so the variant lies within an exon that is part of a UTR
            getLogWriter().write("************************* Variant in Non-protein coding exon region ************************\n");
            insertVariantTranscript(variantId, transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite);
            return true; // true denotes successful insert into VARIANT_TRANSCRIPT
        }
    }

    boolean handleTranslatedProtein(StringBuffer refDna, StringBuffer varDna, int variantRelPos, long variantId,
            int transcriptRgdId, String transcriptLocation, String nearSpliceSite, String transcriptErrorFound,String chr) throws Exception {

        String rnaRefTranslated = translate(refDna);
        String rnaVarTranslated = translate(varDna);

        getLogWriter().write("RNA REF  \n" + rnaRefTranslated + "\n");
        getLogWriter().write("RNA VAR  \n" + rnaVarTranslated + "\n");
        getLogWriter().write("variantRelPos = " + variantRelPos + "\n");

        // Determine AA Symbol
        int pos = 1 + (variantRelPos - 1) / 3;
        getLogWriter().write("Position were looking for: " + pos + " rnaRefTranslate.length : " + rnaRefTranslated.length() + "\n");

        // Check if the variant still falls in the transcript
        if (pos>0 && pos <= rnaRefTranslated.length() && pos <= rnaVarTranslated.length()) {
            String LRef = rnaRefTranslated.substring(pos-1, pos);
            String LVar = rnaVarTranslated.substring(pos-1, pos);

            getLogWriter().write("Calculated  Ref AA = " + LRef + " Var AA = " + LVar + "\n");
            String synStatus = LRef.equals(LVar) ? "synonymous" : "nonsynonymous";
            if( LRef.equals("X") || LVar.equals("X") ) {
                synStatus = "unassignable";
            }

            // compute frameshift
            // compute length difference between reference and variant nucleotides
            int lenDiffRefVar = Math.abs(refDna.length() - varDna.length());
            int reminder = lenDiffRefVar%3;
            String isFrameShift = reminder!=0 ? "T" : "F";

            insertVariantTranscript(variantId, transcriptRgdId, LRef, LVar,
                    synStatus, transcriptLocation, nearSpliceSite, pos, variantRelPos, transcriptErrorFound,
                    rnaRefTranslated, refDna.toString(), isFrameShift,chr);

            return true; // true denotes successful insert into VARIANT_TRANSCRIPT
        } else {
            getLogWriter().write("Variant skipped because it is no longer in the truncated transcript.");
            return false; // return false to insert new row into VARIANT_TRANSCRIPT: at least variant location will be available
        }
    }

    void handleUTRs(ArrayList<Feature> exomsArray, Feature threeUtr, Feature fiveUtr) throws Exception {
        for (Feature feature: exomsArray) {

            // if we have a 3'utr to deal with  at end
            if (threeUtr != null) {
                //  println  "comparing 3prime: " <<  threeUtr.start << "," << threeUtr.stop << " feature: " <<  feature.start << ", " << feature.stop
                if (feature.stop < threeUtr.start) {
                    // use entire feature
                } else if (feature.start < threeUtr.start) {
                    getLogWriter().write("feature start reset to " + (threeUtr.start - 1) + "\n");
                    feature.stop = threeUtr.start - 1;
                } else {
                    // remove all of exome
                    getLogWriter().write("3Prime removed  feature \n");
                    feature.start = -1;
                    feature.stop = -1;
                }
            }
            // if we have a 5' utr to deal with at start
            if (fiveUtr != null) {
                // println  "comparing 5prime: " <<  fiveUtr.start << "," << fiveUtr.stop << " feature: " <<  feature.start << ", " << feature.stop
                if (feature.start > fiveUtr.stop) {
                    // use entire feature
                } else if (feature.stop > fiveUtr.stop) {
                    getLogWriter().write("feature stop reset to " + (fiveUtr.stop + 1) + "\n");
                    feature.start = fiveUtr.stop + 1;
                } else {
                    // remove all of exome
                    getLogWriter().write("5Prime removed feature" + "\n");
                    feature.start = -1;
                    feature.stop = -1;
                }
            }
        }
    }

    static public StringBuffer reverseComplement(CharSequence dna, int transcriptRgdId) throws Exception {

        StringBuffer buf = new StringBuffer(dna.length());
        for( int i=dna.length()-1; i>=0; i-- ) {
            char ch = dna.charAt(i);
            if( ch=='A' || ch=='a' ) {
                buf.append('T');
            } else if( ch=='C' || ch=='c' ) {
                buf.append('G');
            } else if( ch=='G' || ch=='g' ) {
                buf.append('C');
            } else if( ch=='T' || ch=='t' ) {
                buf.append('A');
            } else if( ch=='N' || ch=='n' ) {
                buf.append('N');
            }
            else throw new Exception("reverseComplement: unexpected nucleotide ["+ch+"] with transcript_rgd_id="+transcriptRgdId);
        }
        return buf;
    }

    static
    String translate(StringBuffer dna) {

        StringBuilder out = new StringBuilder(dna.length() / 3);
        for( int i=0; i<dna.length(); i+=3 ) {
            char c1 = Character.toUpperCase(dna.charAt(i + 0));
            char c2 = Character.toUpperCase(dna.charAt(i + 1));
            char c3 = Character.toUpperCase(dna.charAt(i + 2));

            if( c1=='C') { // QUARTER C
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("H"); // histodine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("Q"); // glutamine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("P"); // proline
                }
                else if( c2=='G' ) {
                    out.append("R"); // arginine
                }
                else if( c2=='T' ) {
                    out.append("L"); // leucine
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='G' ) { // QUARTER G
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("D"); // aspartic acid
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("E"); // glutamic acid
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("A"); // alanine
                }
                else if( c2=='G' ) {
                    out.append("G"); // glycine
                }
                else if( c2=='T' ) {
                    out.append("V"); // valine
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='A' ) { // QUARTER A
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("N"); // asparagine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("K"); // lysine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("T"); // threonine
                }
                else if( c2=='G' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("S"); // serine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("R"); // arginine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='T' ) {
                    if( c3=='T' || c3=='C' || c3=='A' ) {
                        out.append("I"); // isoleucine
                    }
                    else if( c3=='G' ) {
                        out.append("M"); // methionine
                    }
                    else
                        out.append("X"); // unknown
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='T' ) { // QUARTER T
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("Y"); // tyrosine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("*"); // STOP
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("S"); // serine
                }
                else if( c2=='G' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("C"); // cysteine
                    }
                    else if( c3=='A' ) {
                        out.append("*"); // STOP
                    }
                    else if( c3=='G' ) {
                        out.append("W"); // tryptophan
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='T' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("F"); // phenylalanine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("L"); // leucine
                    }
                    else
                        out.append("X"); // unknown
                }
                else
                    out.append("X"); // unknown
            }

            else { // QUARTER N
                out.append("X"); // unknown
            }
        }

        return out.toString();
    }

    // **************
    // ***DAO*** code

    void insertVariantTranscript(long variantId, int transcriptRgdId, String transcriptLocation, String nearSpliceSite) throws Exception {
        insertVariantTranscript(variantId, transcriptRgdId, null, null, null, transcriptLocation, nearSpliceSite, null,
                null, null, null, null, null,null);
        getLogWriter().write("		Found variant at Location  " + transcriptLocation + " found for " + variantId
                + ", " + transcriptRgdId + " \n");
        //logStatusMsg("		Found variant for variantId " + variantId + ", transcriptId " + transcriptRgdId + " \n");
    }

    void insertVariantTranscript(long variantId, int transcriptRgdId, String refAA, String varAA, String synStatus,
                                 String transcriptLocation, String nearSpliceSite, Integer fullRefAaPos, Integer fullRefNucPos,
                                 String tripletError, String fullRefAA, String fullRefNuc, String frameShift,String chr) throws Exception {


        int fullRefAASeqKey;
        int fullRefNucSeqKey;
        SequenceDAO sequenceDAO = new SequenceDAO();
        VariantTranscript vt = new VariantTranscript();
        vt.setVariantId(variantId);
        vt.setTranscriptRgdId(transcriptRgdId);
        vt.setRefAA(refAA);
        vt.setVarAA(varAA);
        vt.setSynStatus(synStatus);
        vt.setLocationName(transcriptLocation);
        vt.setNearSpliceSite(nearSpliceSite);
        vt.setFullRefAAPos(fullRefAaPos);
        vt.setFullRefNucPos(fullRefNucPos);
        vt.setTripletError(tripletError);
        String assembly = MapManager.getInstance().getMap(mapKey).getUcscAssemblyId();

        if(fullRefAA != null) {
            Sequence seq = new Sequence();
            seq.setRgdId(transcriptRgdId);
            seq.setSeqData(fullRefAA);
            List<Sequence> aaseqs = sequenceDAO.getObjectSequences(transcriptRgdId, "full_ref_aa");
            if (!aaseqs.isEmpty()) {
                String s = aaseqs.get(0).getSeqData();
                if(s.equalsIgnoreCase(fullRefAA)) {
                    fullRefAASeqKey = aaseqs.get(0).getSeqKey();
                }
                else{
                    aaseqs = sequenceDAO.getObjectSequences(transcriptRgdId, "full_ref_aa_"+assembly);
                    if(aaseqs.isEmpty()) {
                        aaseqs = sequenceDAO.getObjectSequences(transcriptRgdId,"full_ref_aa_"+assembly+"_"+chr);
                        if(aaseqs.isEmpty()) {
                            seq.setSeqType("full_ref_aa_" + assembly);
                            fullRefAASeqKey = sequenceDAO.insertSequence(seq);
                        }else fullRefAASeqKey = aaseqs.get(0).getSeqKey();
                    }else {
                        s = aaseqs.get(0).getSeqData();
                        if (s.equalsIgnoreCase(fullRefAA)) {
                            fullRefAASeqKey = aaseqs.get(0).getSeqKey();
                        }else {
                            seq.setSeqType("full_ref_aa_" + assembly+"_"+chr);
                            fullRefAASeqKey = sequenceDAO.insertSequence(seq);
                        }
                    }
                }
                vt.setFullRefAASeqKey(fullRefAASeqKey);
            }else{
                seq.setSeqType("full_ref_aa");
                fullRefAASeqKey = sequenceDAO.insertSequence(seq);
                vt.setFullRefAASeqKey(fullRefAASeqKey);
            }

        }
        if(fullRefNuc != null) {
            List<Sequence> nucSeqs = sequenceDAO.getObjectSequences(transcriptRgdId, "full_ref_nuc");
            Sequence seq = new Sequence();
            seq.setSeqData(fullRefNuc);
            seq.setRgdId(transcriptRgdId);
            if (nucSeqs.isEmpty()) {
                seq.setSeqType("full_ref_nuc");
                fullRefNucSeqKey = sequenceDAO.insertSequence(seq);
                vt.setFullRefNucSeqKey(fullRefNucSeqKey);
            } else {
                String s = nucSeqs.get(0).getSeqData();
                if(s.equalsIgnoreCase(fullRefNuc)) {
                    fullRefNucSeqKey = nucSeqs.get(0).getSeqKey();
                }else {
                    nucSeqs = sequenceDAO.getObjectSequences(transcriptRgdId,"full_ref_nuc_"+assembly);
                    if(nucSeqs.isEmpty()) {
                        seq.setSeqType("full_ref_nuc_" + assembly);
                        fullRefNucSeqKey = sequenceDAO.insertSequence(seq);
                    }else fullRefNucSeqKey = nucSeqs.get(0).getSeqKey();
                }
                vt.setFullRefNucSeqKey(fullRefNucSeqKey);
            }

        }
        vt.setFrameShift(frameShift);
        vt.setMapKey(mapKey);
        batch.addToBatch(vt);
    }

    void writeError(String msg, int mapKey) throws IOException {
        File errorFile = new File(getLogDir()+"/error_rna_$"+mapKey+".txt");
        FileWriter efile = new FileWriter(errorFile, true);
        efile.write(msg);
        efile.close();
    }

    List<String> getChromosomes(int mapKey) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM variant_map_data WHERE map_key=? ";
        StringListQuery q = new StringListQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        return q.execute(new Object[]{mapKey});
    }

    PreparedStatement psVariant;
    PreparedStatement psTranscript;
    PreparedStatement psExonCount;
    PreparedStatement psTranscriptFeatures;

    void prepareStatements() throws Exception {
        System.out.println("preparing sql statements");

        String sql = "SELECT v.rgd_id,vm.start_pos,vm.end_pos,v.var_nuc,v.ref_nuc "+
                "FROM variant v inner join variant_map_data vm on v.rgd_id = vm.rgd_id and vm.map_key = ? and vm.chromosome = ?";
        psVariant = getVariantDataSource().getConnection().prepareStatement(sql);


  /*      sql = "SELECT transcript_rgd_id,is_non_coding_ind FROM transcripts WHERE gene_rgd_id=? "+
        "AND EXISTS(SELECT 1 FROM maps_data md WHERE md.rgd_id=transcript_rgd_id AND md.map_key=?)";
        psTranscript = getDataSource().getConnection().prepareStatement(sql);


        sql = "SELECT count(*) "+
            "FROM TRANSCRIPT_FEATURES tf, rgd_ids r, maps_data md "+
            "WHERE "+
            " tf.FEATURE_RGD_ID = r.rgd_id "+
            " AND tf.FEATURE_RGD_ID = md.RGD_ID "+
            " AND tf.TRANSCRIPT_RGD_ID=?"+
            " AND md.MAP_KEY=?"+
            " AND md.CHROMOSOME=?"+
            " AND r.OBJECT_KEY=15"; // "EXONS"
        psExonCount = getDataSource().getConnection().prepareStatement(sql);


        sql = "SELECT" +
                "    ro.OBJECT_NAME,\n" +
                "    md.STRAND,\n" +
                "    md.CHROMOSOME,\n" +
                "    md.START_POS,\n" +
                "    md.STOP_POS\n" +
                "FROM\n" +
                "    TRANSCRIPT_FEATURES tf ,\n" +
                "    rgd_ids r,\n" +
                "    maps_data md,\n" +
                "    rgd_objects ro\n" +
                "WHERE\n" +
                "    tf.FEATURE_RGD_ID = r.rgd_id\n" +
                "AND tf.FEATURE_RGD_ID = md.RGD_ID\n" +
                "AND tf.TRANSCRIPT_RGD_ID = ?\n" +
                "AND md.MAP_KEY = ?\n" +
                "AND md.CHROMOSOME = ?\n" +
                "AND r.OBJECT_KEY = ro.OBJECT_KEY order by OBJECT_NAME, START_POS, STOP_POS";

        psTranscriptFeatures = getDataSource().getConnection().prepareStatement(sql);
        */
    }

    void closePreparedStatements() throws Exception {
        psVariant.close();
    /*    psTranscript.close();
        psExonCount.close();
        psTranscriptFeatures.close();
    */
    }

    ResultSet getVariantResultSet(int mapKey, String chr) throws Exception {

        psVariant.setInt(1, mapKey);
        psVariant.setString(2, chr);
        return psVariant.executeQuery();
    }

    // Get all transcripts for gene
/*    ResultSet getTranscriptsResultSet(int geneRgdId, int mapKey) throws Exception {

        psTranscript.setInt(1, geneRgdId);
        psTranscript.setInt(2, mapKey);
        return psTranscript.executeQuery();
    }
*/
    int getExonCount(int transcriptRgdId, String chr, int mapKey) throws Exception {

   /*     psExonCount.setInt(1, transcriptRgdId);
        psExonCount.setInt(2, mapKey);
        psExonCount.setString(3, chr);
        ResultSet rs = psExonCount.executeQuery();
        rs.next();
        int exonCount = rs.getInt(1);
        rs.close();
    */
        if(transcriptCache.exonResult.containsKey(transcriptRgdId))
           return transcriptCache.exonResult.get(transcriptRgdId);
        else return 0;
    }

    // Get all Transcript features for this transcript. These are Exoms, 3primeUTRs and 5PrimeUTRs
    ResultSet getTranscriptFeaturesResultSet(int transcriptRgdId, String chr, int mapKey) throws Exception {

        psTranscriptFeatures.setInt(1, transcriptRgdId);
        psTranscriptFeatures.setInt(2, mapKey);
        psTranscriptFeatures.setString(3, chr);
        return psTranscriptFeatures.executeQuery();
    }

    /*
    ////////////////////////////////////////////////////
    // diagnostics routine to verify AA
    public void verifyAA(int sampleId) throws Exception {

        appendMessageToVarAaFixLog(sampleId, "------START--------");

        String sql = "SELECT v.ref_nuc,v.var_nuc,vt.* "+
                "FROM variant v,variant_transcript vt WHERE v.variant_id=vt.variant_id AND syn_status IS NOT NULL AND sample_id=?";
        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, sampleId);
        ResultSet rs = ps.executeQuery();
        int matches = 0;
        int mismatches = 0;
        int skipped = 0;

        int synmatch = 0;
        int synmismatch = 0;
        int nonsynmismatch = 0;
        int negstrand = 0;
        int synXXmatch = 0;

        while( rs.next() ) {
            long varTrId = rs.getLong("variant_transcript_id");
            String refAA = rs.getString("ref_aa");
            String varAA = rs.getString("var_aa");
            int nucPos = rs.getInt("full_ref_nuc_pos");
            int aaPos = rs.getInt("full_ref_aa_pos");
            String varNuc = rs.getString("var_nuc");
            String fullRefNuc = rs.getString("full_ref_nuc");
            String synStatus = rs.getString("syn_status");

            int codonPos = 3*((nucPos-1)/3); // 0-based
            if( codonPos+2 >= fullRefNuc.length() ) {
                System.out.println("codon pos problem");
                continue;
            }
            String codonRef = fullRefNuc.substring(codonPos, codonPos+3);
            String aaRefTranslated = translate(new StringBuffer(codonRef));
            StringBuffer codonVar = null;


            int trRgdId = rs.getInt("transcript_rgd_id");
            int strand = getStrandForTranscript(trRgdId, sampleId<700?60:70);
            if( strand==0 ) {
                System.out.println("no strand info");
                skipped++;
                continue;
            }
            codonVar = new StringBuffer(codonRef);

            if( strand<0 ) {
                negstrand++;

                // var_nuc and REF_NUC must be reverse-complemented
                // (nuc and positions as well as full dna/aa is already reverse-complemented
                codonVar.setCharAt((nucPos-1)%3, reverseComplement(new StringBuffer(varNuc)).charAt(0));
            }
            else if( strand>0 ) { // positive strand
                codonVar.setCharAt((nucPos-1)%3, varNuc.charAt(0));
            }
            String aaVarTranslated = translate(codonVar);


            boolean synProblem = false;
            if( synStatus.equals("synonymous") && !aaRefTranslated.equals(aaVarTranslated) ) {
                System.out.println("synonymous mismatch, strand "+strand);
                synmismatch++;
                synProblem = true;
            }
            if( synStatus.equals("nonsynonymous") && aaRefTranslated.equals(aaVarTranslated) ) {
                System.out.println("nonsynonymous mismatch, strand "+strand);
                nonsynmismatch++;
                synProblem = true;
            }
            if( !synProblem )
                synmatch++;

            if( varAA.equals(aaVarTranslated) ) {
                matches++;
                if( synProblem ) {
                    if( refAA.equals("X") || varAA.equals("X") )
                        synXXmatch++;
                    else
                        flipSynStatus(sampleId, varTrId, synStatus);
                }
            }
            else {
                mismatches++;
                updateVarAA(sampleId,varTrId,varAA,aaVarTranslated);
            }
        }
        conn.close();

        //System.out.println("Sample "+sampleId+" neg strand="+negstrand);
        System.out.println("Sample " + sampleId + " skipped=" + skipped + " (cannot determine strand)");
        System.out.println("Sample "+sampleId+" matches="+matches);
        System.out.println("Sample "+sampleId+" mismatches="+mismatches+" (fixed in db)");
        System.out.println(" out of these");
        System.out.println("   synonymous/nonsynomous status matches="+synmatch);
        System.out.println("   synonymous status mismatches="+synmismatch);
        System.out.println("   nonsynonymous status mismatches="+nonsynmismatch);
        System.out.println("   synonymous status XX mismatches="+synXXmatch);

        appendMessageToVarAaFixLog(sampleId, "Sample " + sampleId + " skipped=" + skipped + " (cannot determine strand)");
        appendMessageToVarAaFixLog(sampleId, "Sample "+sampleId+" matches="+matches);
        appendMessageToVarAaFixLog(sampleId, "Sample "+sampleId+" mismatches="+mismatches+" (fixed in db)");
        appendMessageToVarAaFixLog(sampleId, " out of these");
        appendMessageToVarAaFixLog(sampleId, "   synonymous/nonsynomous status matches="+synmatch);
        appendMessageToVarAaFixLog(sampleId, "   synonymous status mismatches="+synmismatch);
        appendMessageToVarAaFixLog(sampleId, "   nonsynonymous status mismatches="+nonsynmismatch);
        appendMessageToVarAaFixLog(sampleId, "   synonymous status XX mismatches="+synXXmatch);
        appendMessageToVarAaFixLog(sampleId, "------DONE---------");
    }

    int getStrandForTranscript(int trRgdId, int mapKey) throws Exception {
        String sql = "select strand "+
            "from transcripts tr,genes g,maps_data m "+
            "where transcript_rgd_id=? "+
            "and gene_rgd_id=g.rgd_id and g.rgd_id=m.rgd_id and map_key=? and strand is not null";

        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, trRgdId);
        ps.setInt(2, mapKey);
        ResultSet rs = ps.executeQuery();
        int strand = 0;
        while( rs.next() ) {
            String s = rs.getString("strand");
            if( s.equals("+") ) {
                if( strand<0 )
                    System.out.println("strand conflict");
                strand = 1;
            }
            if( s.equals("-") ) {
                if( strand>0 )
                    System.out.println("strand conflict");
                strand = -1;
            }
        }
        conn.close();

        if( strand==0 ) { // try for any map_key

            sql = "select strand "+
                "from transcripts tr,genes g,maps_data m "+
                "where transcript_rgd_id=? "+
                "and gene_rgd_id=g.rgd_id and g.rgd_id=m.rgd_id and strand is not null";

            conn = this.getDataSource().getConnection();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, trRgdId);
            rs = ps.executeQuery();
            strand = 0;
            while( rs.next() ) {
                String s = rs.getString("strand");
                if( s.equals("+") ) {
                    if( strand<0 )
                        System.out.println("strand conflict");
                    strand = 1;
                }
                if( s.equals("-") ) {
                    if( strand>0 )
                        System.out.println("strand conflict");
                    strand = -1;
                }
            }
            conn.close();
        }
        return strand;
    }
    */
    void updateVarAA(int sampleId, long varTrId, String oldVarAA, String newVarAA) throws Exception {

        String sql = "update variant_transcript "+
                "set var_aa=? where var_aa=? and variant_transcript_id=? ";

        /*
        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, newVarAA);
        ps.setString(2, oldVarAA);
        ps.setLong(3, varTrId);
        ps.execute();
        int updateCount = ps.getUpdateCount();
        conn.close();
        */
        String msg = "variant_transcript_id="+varTrId+", old VAR_AA="+oldVarAA+", new VAR_AA="+newVarAA;
        appendMessageToVarAaFixLog(sampleId, msg);
    }

    void flipSynStatus(int mapKey, long varTrId, String oldSynStatus) throws Exception {

        String newSynStatus = oldSynStatus.equals("synonymous") ? "nonsynonymous" :
                oldSynStatus.equals("nonsynonymous") ? "synonymous" :
                        oldSynStatus;
/*
        String sql = "update variant_transcript "+
            "set syn_status=? where syn_status=? and variant_transcript_id=? ";

        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, newSynStatus);
        ps.setString(2, oldSynStatus);
        ps.setLong(3, varTrId);
        ps.execute();
        int updateCount = ps.getUpdateCount();
        conn.close();
  */
        String msg = "variant_rgd_id="+varTrId+", old SYN_STATUS="+oldSynStatus+", new SYN_STATUS="+newSynStatus;
        appendMessageToVarAaFixLog(mapKey, msg);
    }

    void appendMessageToVarAaFixLog(int sampleId, String msg) throws Exception {

        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/"+sampleId+"_varAA_fix.log", true));
        writer.write(msg);
        writer.newLine();
        writer.close();
    }

    public String getDnaChunk(FastaParser parser, int start, int stop) throws Exception {
        String key = start+"+"+stop;
        String dna = dnaCache.get(key);
        if( dna==null ) {
            dna = getDnaChunkFromFastaFile(parser, start, stop);
            dnaCache.put(key, dna);
        }
        return dna;
    }

    public String getDnaChunkFromFastaFile(FastaParser parser, int start, int stop) throws Exception {
        String fasta = parser.getSequence(start, stop);
        return fasta.replaceAll("\\s+", "");
    }

    Map<String,String> dnaCache = new HashMap<>();

    public void initGene(int geneRgdId) {
        // the idea is to keep in the cache positions for gene exons
        // the more transcripts a gene has, the bigger benefits of this cache
        dnaCache.clear();
    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setFastaDir(String fastaDir) {
        this.fastaDir = fastaDir;
    }

    public String getFastaDir() {
        return fastaDir;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public String getLogDir() {
        return logDir;
    }

    class Feature {
        public int start;
        public int stop;
        public String location; // 3UTRS, 5UTRS, EXONS

        public Feature(int start, int stop, String location) {
            this.start = start;
            this.stop = stop;
            this.location = location;
        }
    }

    class TranscriptFlags {
        public String strand = null;
        public Feature threeUtr = null;
        public Feature fiveUtr = null;
        public ArrayList<Feature> exomsArray = new ArrayList<Feature>();
        // process all transcripts 3UTS and 5UTR are top of the list of results but don't count on getting
        // them every time in results
        public String nearSpliceSite = "F";
        public String transcriptLocation = null;
        public boolean inExon = false;
    }

    String getProperChunk(FastaParser fastaFile, int transcriptRgdId, String chr, int start, int stop, int mapKey) throws Exception{
        String newDnaChunk = "";
        Transcript t = tdao.getTranscript(transcriptRgdId);
        List<MapData> mapData = mdao.getMapData(t.getRgdId(), mapKey);
        if (mapData.isEmpty())
            return getDnaChunk(fastaFile, start,stop);
        for (MapData m : mapData) {
             if (!m.getChromosome().equals(chr) && start==m.getStartPos()){
                fastaFile.setChr(m.getChromosome());
                newDnaChunk = getDnaChunk(fastaFile,m.getStartPos(),m.getStopPos());
                fastaFile.setChr(chr);
                break;
            }
        }
        if (newDnaChunk.isEmpty())
            return getDnaChunk(fastaFile, start,stop);

        return newDnaChunk;

    }
}