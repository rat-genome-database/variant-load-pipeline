package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.process.Utils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author mtutaj
 * @since 11/20/13
 */
public class Polyphen2 extends VariantProcessingBase {

    private String version;

    private String WORKING_DIR = "/data/rat/polyphen";
    private BufferedWriter errorFile;
    private BufferedWriter polyphenFile;
    private BufferedWriter polyphenFileInfo;
    private BufferedWriter noSeqFile;
    private BufferedWriter fastaFile;

    private List<String> polyphenFileLines = new ArrayList<String>();

    SequenceDAO sequenceDAO = new SequenceDAO();

    public Polyphen2() throws Exception {

    }

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Polyphen2 instance = (Polyphen2) (bf.getBean("polyphen2"));

        // process args
        int sampleId = 0;
        Set<String> geneSymbols = new HashSet<String>();

        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--sample") || args[i].equals("-s") ) {
                sampleId = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("--gene") || args[i].equals("-g") ) {
                geneSymbols.add(args[++i]);
            }
        }

        instance.run(sampleId, geneSymbols);

        instance.getLogWriter().close();
    }

    public void run(int sampleId, Collection<String> geneSymbols) throws Exception {

        String fileNameBase = WORKING_DIR + "/" + sampleId;
        this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".detail.log")));

        String errorFileName = WORKING_DIR + "/" + sampleId + ".error";
        errorFile = new BufferedWriter(new FileWriter(errorFileName));

        String polyphenFileName = WORKING_DIR + "/" + sampleId + ".PolyPhenInput";
        this.getLogWriter().write("starting "+polyphenFileName+"\n");
        polyphenFileInfo = new BufferedWriter(new FileWriter(polyphenFileName+".info"));
        polyphenFileInfo.append("#Note: if STRAND is '-', then inverted NUC_VAR is AA_REF\n");
        polyphenFileInfo.append("#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID\n");

        noSeqFile = new BufferedWriter(new FileWriter(polyphenFileName+".noseq"));
        fastaFile = new BufferedWriter(new FileWriter(polyphenFileName+".fasta"));

        runSample(sampleId, geneSymbols);

        polyphenFileInfo.close();
        errorFile.close();
        noSeqFile.close();
        fastaFile.close();

        // write all lines to polyphen input file
        // but first randomize them in order to have more even load during polyphen processing
        Collections.shuffle(polyphenFileLines);
        polyphenFile = new BufferedWriter(new FileWriter(polyphenFileName));
        for( String line: polyphenFileLines ) {
            polyphenFile.append(line);
        }
        polyphenFile.close();

        this.getLogWriter().write("finished " + polyphenFileName + "\n\n\n");
    }

    public void runSample(int sampleId, Collection<String> geneSymbols) throws Exception {

        VariantDAO vdao = new VariantDAO();
        String varTable = vdao.getVariantTable(sampleId);
        String vtTable = vdao.getVariantTranscriptTable(sampleId);
        int variantsProcessed = 0;
        int refSeqProteinLengthErrors = 0;
        int refSeqProteinLeftPartMismatch = 0;
        int refSeqProteinRightPartMismatch = 0;
        int proteinRefSeqNotInRgd = 0;

        // /*+ INDEX(vt) */
        // this query hint forces oracle to use indexes on VARIANT_TRANSCRIPT table
        // (many times it was using full scans for unknown reasons)

        String sql = "SELECT /*+ INDEX(vt) */ \n" +
        "vt.variant_transcript_id, v.start_pos, g.gene_symbol as region_name, \n" +
        "vt.ref_aa, vt.var_aa, vt.full_ref_aa, vt.full_ref_aa_pos, \n" +
        "t.acc_id, t.protein_acc_id, vt.transcript_rgd_id, v.variant_id, s.map_key, v.chromosome\n" +
        "FROM sample s,"+varTable+" v, "+vtTable+" vt, transcripts t, genes g\n" +
        "WHERE vt.ref_aa <> vt.var_aa  AND  vt.var_aa<>'*' \n" +
        "AND v.ref_nuc IN ('A', 'G', 'C', 'T') \n" +
        "AND v.var_nuc IN ('A', 'G', 'C', 'T') \n" +
        "AND vt.ref_aa IS NOT NULL  AND  vt.var_aa IS NOT NULL \n" +
        "AND s.sample_id = ? \n" +
        "AND v.variant_id = vt.variant_id \n" +
        "AND t.transcript_rgd_id=vt.transcript_rgd_id \n" +
        "AND t.gene_rgd_id=g.rgd_id \n" +
        "AND v.sample_id=s.sample_id";
        if( !geneSymbols.isEmpty() )
            sql += " AND g.gene_symbol IN("+ Utils.concatenate(geneSymbols,",","'")+")";

        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, sampleId);

        ResultSet rs = ps.executeQuery();
        int lineNr = 0;
        String line;
        while( rs.next() ) {
            lineNr++;
            long variantTranscriptId = rs.getLong(1);
            int startPos = rs.getInt(2);
            String regionName = rs.getString(3);
            String refAA = rs.getString(4);
            String varAA = rs.getString(5);
            String fullRefAA = rs.getString(6);
            int fullRefAaaPos = rs.getInt(7);
            String nucAccId = rs.getString(8);
            String proteinAccId = rs.getString(9);
            int transcriptRgdId = rs.getInt(10);
            long variantId = rs.getLong(11);
            int mapKey = rs.getInt(12);
            String chr = rs.getString(13);

            String strand = getStrand(transcriptRgdId, chr, startPos, mapKey);

            this.getLogWriter().append("\n\nChr " + chr + " line " + lineNr + "\n" +
                    "    variant_id = " + variantId + "\n" +
                    "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                    "    protein_acc_id = " + proteinAccId + "\n" +
                    "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                    "    ref_aa = " + refAA + "\n" +
                    "    var_aa = " + varAA + "\n" +
                    "    region_name = " + regionName + "\n" +
                    "    strand = " + strand + "\n");

            // retrieve protein sequence from RGD
            List<Sequence> seqsInRgd = getProteinSequences(transcriptRgdId);
            if( seqsInRgd==null || seqsInRgd.isEmpty() ) {
                proteinRefSeqNotInRgd++;
                reportProteinSeqNotInRgd(nucAccId, proteinAccId);
                continue;
            }

            for( Sequence seq: seqsInRgd ) {

                // RefSeq protein part to the left of the mutation point must match the translated part
                String refSeqLeftPart;
                String translatedLeftPart = fullRefAA.substring(0, fullRefAaaPos-1);
                try {
                    refSeqLeftPart = seq.getSeqData().substring(0, fullRefAaaPos-1);
                } catch(IndexOutOfBoundsException e) {
                    line = "RefSeq protein shorter than REF_AA_POS!\n" +
                            "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                            "    protein_acc_id = " + proteinAccId + "\n" +
                            "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                            "    RefSeq protein length = " + seq.getSeqData().length() + "\n";
                    errorFile.append(line);
                    refSeqProteinLengthErrors++;
                    this.getLogWriter().append("***LEFT FLANK LENGTH ERROR***\n" + line);
                    continue;
                }

                if( !refSeqLeftPart.equalsIgnoreCase(translatedLeftPart) ) {
                    line = "Left flank not the same!\n" +
                            "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                            "    protein_acc_id = " + proteinAccId + "\n" +
                            "    RefSeq left part         " + refSeqLeftPart + "\n" +
                            "    translated ref left part " + translatedLeftPart + "\n";
                    errorFile.append(line);
                    refSeqProteinLeftPartMismatch++;
                    this.getLogWriter().append("***LEFT FLANK ERROR***\n"+line);
                    continue;
                }


                // RefSeq protein part to the right of the mutation point must match the translated part
                String refSeqRightPart;
                String translatedRightPart = fullRefAA.substring(fullRefAaaPos);
                if( translatedRightPart.endsWith("*") )
                    translatedRightPart = translatedRightPart.substring(0, translatedRightPart.length()-1);
                try {
                    refSeqRightPart = seq.getSeqData().substring(fullRefAaaPos);
                } catch(IndexOutOfBoundsException e) {
                    line = "RefSeq protein shorter than REF_AA_POS!\n" +
                            "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                            "    protein_acc_id = " + proteinAccId + "\n" +
                            "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                            "    RefSeq protein length = " + seq.getSeqData().length() + "\n";
                    errorFile.append(line);
                    refSeqProteinLengthErrors++;
                    this.getLogWriter().append("***RIGHT FLANK LENGTH ERROR***\n"+line);
                    continue;
                }

                if( !refSeqRightPart.equalsIgnoreCase(translatedRightPart) ) {
                    line = "Right flank not the same!\n" +
                            "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                            "    protein_acc_id = " + proteinAccId + "\n" +
                            "    RefSeq left part         " + refSeqRightPart + "\n" +
                            "    translated ref right part " + translatedRightPart + "\n";
                    errorFile.append(line);
                    refSeqProteinRightPartMismatch++;
                    this.getLogWriter().append("***RIGHT FLANK ERROR***\n"+line);
                    continue;
                }

                this.getLogWriter().append("***MATCH***\n"+
                        "  proteinLeftPart\n" + refSeqLeftPart+"\n"+
                        "  proteinRightPart\n" + refSeqRightPart+"\n");

                variantsProcessed++;

                // write polyphen input file
                // PROTEIN_ACC_ID POS REF_AA VAR_AA
                line = "RGD_"+proteinAccId+" "+fullRefAaaPos+" "+refAA+" "+varAA+"\n";
                polyphenFileLines.add(line);

                // write polyphen input info file
                //#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID");
                line = variantId+"\t"+variantTranscriptId+"\t"+regionName+"\tRGD_"+proteinAccId+"\t"+fullRefAaaPos+"\t"+refAA+"\t"+varAA+"\t"+strand+"\t"+transcriptRgdId+"\n";
                polyphenFileInfo.write(line);

                writeFastaSequence("RGD_"+proteinAccId, seq.getSeqData());
            }
        }

        conn.close();

        getLogWriter().write("\nPROCESSING SUMMARY:");
        getLogWriter().write("\n  toBeProcessedByPolyphen = "+variantsProcessed);
        getLogWriter().write("\n  refSeqProteinLengthErrors = "+refSeqProteinLengthErrors);
        getLogWriter().write("\n  refSeqProteinLeftPartMismatch = "+refSeqProteinLeftPartMismatch);
        getLogWriter().write("\n  refSeqProteinRightPartMismatch = "+refSeqProteinRightPartMismatch);
        getLogWriter().write("\n  proteinRefSeqNotInRgd = "+proteinRefSeqNotInRgd);
        getLogWriter().write("\n  TOTAL VARIANT_TRANSCRIPT ROWS PROCESSED = "+(proteinRefSeqNotInRgd+variantsProcessed+
            refSeqProteinLengthErrors+refSeqProteinLeftPartMismatch+refSeqProteinRightPartMismatch));
        getLogWriter().write("\n");
    }

    List<Sequence> getProteinSequences(int transcriptRgdId) throws Exception {
        return sequenceDAO.getObjectSequences(transcriptRgdId, "ncbi_protein");
    }

    // ensure we write fasta sequences only once
    Set<String> fastaProteinAccIds = new HashSet<String>();

    void writeFastaSequence(String proteinAccId, String proteinSeq) throws Exception {
        if( !fastaProteinAccIds.add(proteinAccId) )
            return;

        fastaFile.write(">"+proteinAccId);
        fastaFile.newLine();

        // write protein fasta sequence, up to 70 characters per line
        for( int i=0; i< proteinSeq.length(); i+=70 ) {
            int chunkEndPos = i+70;
            if( chunkEndPos > proteinSeq.length() )
                chunkEndPos = proteinSeq.length();
            String chunk = proteinSeq.substring(i, chunkEndPos);
            fastaFile.write(chunk);
            fastaFile.newLine();
        }
    }

    String getStrand(int rgdId, String chr, int pos, int mapKey) throws Exception {

        String strands = "";

        String sql = "SELECT DISTINCT strand FROM maps_data md "+
                "WHERE md.rgd_id=? AND map_key=? AND chromosome=? AND start_pos<=? AND stop_pos>=?";

        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, rgdId);
        ps.setInt(2, mapKey);
        ps.setString(3, chr);
        ps.setInt(4, pos);
        ps.setInt(5, pos);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            String strand = rs.getString(1);
            if( strand != null )
                strands += strand;
        }

        conn.close();
        return strands;
    }

    Set<String> noProtSequences = new HashSet<String>();

    void reportProteinSeqNotInRgd(String nuclAccId, String proteinAccId) throws Exception {

        this.getLogWriter().append("***PROTEIN REFSEQ NOT IN RGD: "+proteinAccId+" "+nuclAccId+" ***\n");

        String key = nuclAccId;
        if( proteinAccId!=null )
            key += "\t" + proteinAccId;
        if( noProtSequences.add(key) ) {
            this.noSeqFile.append(key+"\n");
        }
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
