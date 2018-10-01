package edu.mcw.rgd.ratcn.fixup;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.datamodel.TranscriptFeature;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;

import java.util.*;

/**
 * Created by mtutaj on 4/17/2017.
 */
public class FeatureCount {

    public static void main(String[] args) throws Exception {

        // count features for given assembly
        int mapKey = 360;

        int geneCount = 0;
        int mappedGeneCount = 0; // genes with at least one position on assembly
        int transcriptCount = 0;
        int exonCount = 0;
        int utr3Count = 0;
        int utr5Count = 0;
        int cdsCount = 0;
        int transcriptsWithIssues = 0;

        Map<String,Integer> mappedGeneCounts = new TreeMap<>(); // genes with at least one position on assembly
        Map<String,Integer> transcriptCounts = new TreeMap<>();
        Map<String,Integer> exonCounts = new TreeMap<>();
        Map<String,Integer> utr3Counts = new TreeMap<>();
        Map<String,Integer> utr5Counts = new TreeMap<>();
        Map<String,Integer> cdsCounts = new TreeMap<>();
        Map<String,Integer> transcriptsWithIssuess = new TreeMap<>();

        GeneDAO geneDAO = new GeneDAO();
        MapDAO mapDAO = new MapDAO();
        TranscriptDAO transcriptDAO = new TranscriptDAO();

        List<Gene> genes = geneDAO.getActiveGenes(3);
        geneCount = genes.size();

        for( Gene gene: genes ) {
            List<MapData> mds = mapDAO.getMapData(gene.getRgdId(), mapKey);
            String chrGene = !mds.isEmpty() ? mds.get(0).getChromosome() : null;

            List<Transcript> transcripts = transcriptDAO.getTranscriptsForGene(gene.getRgdId(), mapKey);
            for( Transcript tr: transcripts ) {
                mds = mapDAO.getMapData(tr.getRgdId(), mapKey);
                String chrTr = !mds.isEmpty() ? mds.get(0).getChromosome() : null;

                System.out.println(gene.getSymbol()+" "+tr.getAccId());

                List<TranscriptFeature> features = transcriptDAO.getFeatures(tr.getRgdId(), mapKey);
                StringBuffer error = new StringBuffer();
                List<TranscriptFeature> modelFeatures = getModelFeatures(features, error);
                for( TranscriptFeature tf: modelFeatures ) {
                    if( tf.getFeatureType()== TranscriptFeature.FeatureType.EXON ) {
                        exonCount++;
                        incrementCounter(exonCounts, chrTr);
                    }
                    else if( tf.getFeatureType()== TranscriptFeature.FeatureType.UTR3 ) {
                        utr3Count++;
                        incrementCounter(utr3Counts, chrTr);
                    }
                    else if( tf.getFeatureType()== TranscriptFeature.FeatureType.UTR5 ) {
                        utr5Count++;
                        incrementCounter(utr5Counts, chrTr);
                    }
                    else if( tf.getFeatureType()== TranscriptFeature.FeatureType.CDS ) {
                        cdsCount++;
                        incrementCounter(cdsCounts, chrTr);
                    }
                }
                transcriptCount++;
                incrementCounter(transcriptCounts, chrTr);

                if( error.length()>0 ) {
                    transcriptsWithIssues++;
                    incrementCounter(transcriptsWithIssuess, chrTr);
                }
            }

            if( transcripts.size()>0 ) {
                mappedGeneCount++;
                incrementCounter(mappedGeneCounts, chrGene);
            }
        }

        System.out.println("ASSEMBLY  = "+ MapManager.getInstance().getMap(mapKey).getName());
        System.out.println("gene count        = "+geneCount);
        System.out.println("mapped gene count = "+mappedGeneCount);
        System.out.println("transcript count  = "+transcriptCount);
        System.out.println("exon count        = "+exonCount);
        System.out.println("utr3 count        = "+utr3Count);
        System.out.println("utr5 count        = "+utr5Count);
        System.out.println("cds count         = "+cdsCount);
        System.out.println("transcripts with issues = "+transcriptsWithIssues);

        dumpCounts(mappedGeneCounts, "mapped gene counts per chr");
        dumpCounts(transcriptCounts, "transcript count per chr");
        dumpCounts(exonCounts, "exon count per chr");
        dumpCounts(utr3Counts, "utr3 count per chr");
        dumpCounts(utr5Counts, "utr5 count per chr");
        dumpCounts(cdsCounts, "cds counts per chr");
        dumpCounts(transcriptsWithIssuess, "transcripts with issues per chr");
    }

    static List<TranscriptFeature> getModelFeatures(List<TranscriptFeature> features, StringBuffer error) throws CloneNotSupportedException {

        int transcriptHasIssues = 0;

        List<TranscriptFeature> exons = new ArrayList<>();
        List<TranscriptFeature> utr3 = new ArrayList<>();
        List<TranscriptFeature> utr5 = new ArrayList<>();
        List<TranscriptFeature> cds = new ArrayList<>();


        // get exons
        int utr3Start = 0, utr3Stop = 0;
        int utr5Start = 0, utr5Stop = 0;

        for( TranscriptFeature tf: features ) {
            if( tf.getFeatureType()== TranscriptFeature.FeatureType.EXON ) {
                exons.add(tf);
            }
            else if( tf.getFeatureType()== TranscriptFeature.FeatureType.UTR3 ) {
                utr3Start = tf.getStartPos();
                utr3Stop = tf.getStopPos();
            }
            else if( tf.getFeatureType()== TranscriptFeature.FeatureType.UTR5 ) {
                utr5Start = tf.getStartPos();
                utr5Stop = tf.getStopPos();
            }
        }

        Collections.sort(exons, new Comparator<TranscriptFeature>() {
            @Override
            public int compare(TranscriptFeature o1, TranscriptFeature o2) {
                int r = o1.getChromosome().compareTo(o2.getChromosome());
                if( r!=0 ) {
                    return r;
                }
                r = o1.getStartPos() - o2.getStartPos();
                if( r!=0 ) {
                    return r;
                }
                return o1.getStopPos() - o2.getStopPos();
            }
        });

        for( TranscriptFeature exon: exons ) {
            // case 1: exon overlaps utr3 region
            if( utr3Start>0 && utr3Stop>0 ) {
                int result = handleUtr(utr3Start, utr3Stop, exon, TranscriptFeature.FeatureType.UTR3, utr3, cds);
                if( result<0 ) {
                    transcriptHasIssues++;
                }
                if( result!=0 ) {
                    continue;
                }
            }

            // case 2: exon overlaps utr5 region
            if( utr5Start>0 && utr5Stop>0 ) {
                int result = handleUtr(utr5Start, utr5Stop, exon, TranscriptFeature.FeatureType.UTR5, utr5, cds);
                if( result<0 ) {
                    transcriptHasIssues++;
                }
                if( result!=0 ) {
                    continue;
                }
            }

            TranscriptFeature tfCds = exon.clone();
            tfCds.setFeatureType(TranscriptFeature.FeatureType.CDS);
            cds.add(tfCds);
        }

        if( transcriptHasIssues>0 ) {
            error.append("Transcript has issues");
        }
        List<TranscriptFeature> canonicalFeatures = new ArrayList<>(exons);
        canonicalFeatures.addAll(utr3);
        canonicalFeatures.addAll(utr5);
        canonicalFeatures.addAll(cds);
        return canonicalFeatures;
    }

    //
    static int handleUtr(int utrStart, int utrStop, TranscriptFeature exon, TranscriptFeature.FeatureType ftype,
                          List<TranscriptFeature> utrs, List<TranscriptFeature> cds) throws CloneNotSupportedException{

        if( utrStart==0 || utrStop==0 ) {
            return 0;
        }

        if (exon.getStopPos() >= utrStart  &&  exon.getStartPos()<= utrStop ) {
            // case 1a: utr spans over entire exon
            if( exon.getStartPos()>=utrStart && exon.getStopPos()<=utrStop ) {
                TranscriptFeature tfUtr = exon.clone();
                tfUtr.setFeatureType(ftype);
                utrs.add(tfUtr);
            }
            // case 1b: utr spans left part of an exon
            else if( exon.getStartPos()>=utrStart && exon.getStopPos()>utrStop ) {
                TranscriptFeature tfUtr = exon.clone();
                tfUtr.setFeatureType(ftype);
                tfUtr.setStopPos(utrStop);
                utrs.add(tfUtr);

                TranscriptFeature tfCds = exon.clone();
                tfCds.setFeatureType(TranscriptFeature.FeatureType.CDS);
                tfCds.setStartPos(utrStop+1);
                cds.add(tfCds);
            }
            // case 1c: utr spans right part of an exon
            else if( exon.getStartPos()<utrStart && exon.getStopPos()<=utrStop ) {
                TranscriptFeature tfUtr = exon.clone();
                tfUtr.setFeatureType(ftype);
                tfUtr.setStartPos(utrStart);
                utrs.add(tfUtr);

                TranscriptFeature tfCds = exon.clone();
                tfCds.setFeatureType(TranscriptFeature.FeatureType.CDS);
                tfCds.setStopPos(utrStart-1);
                cds.add(tfCds);
            }
            // case 1d: unexpected
            else {
                return -1;
            }
            return 1;
        }
        return 0;
    }

    static void incrementCounter(Map<String,Integer> map, String chr) {
        chr = Utils.defaultString(chr);

        Integer count = map.get(chr);
        if( count == null ) {
            count = 0;
        }
        map.put(chr, 1+count);
    }

    static void dumpCounts(Map<String,Integer> map, String title) {
        System.out.println();
        System.out.println(title);
        for( Map.Entry<String,Integer> entry: map.entrySet() ) {
            System.out.println("  "+entry.getKey()+"  = "+entry.getValue());
        }
    }
}
