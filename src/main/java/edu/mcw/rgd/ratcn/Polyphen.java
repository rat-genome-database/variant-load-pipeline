package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.mapping.MapManager;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 11/20/13
 */
public class Polyphen extends VariantProcessingBase {

    private String version;

    private String WORKING_DIR = "/data/rat/output";
    private BufferedWriter errorFile;
    private BufferedWriter polyphenFile;
    private BufferedWriter polyphenFileInfo;
    private BufferedWriter fastaFile;

    SequenceDAO sequenceDAO = new SequenceDAO();

    boolean simpleProteinQC = false;
    boolean createFastaFile = false;



    public Polyphen() throws Exception {

    }

    public static void main(String[] args) throws Exception {

        XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        Polyphen instance = (Polyphen) (bf.getBean("polyphen"));


        // process args
        String chr = null;
        int mapKey = 0;
        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--mapKey") ) {
                mapKey = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("--chr") ) {
                chr = args[++i];
            }
            else if( args[i].equals("--outDir") ) {
                instance.WORKING_DIR = args[++i];
            }
            else if( args[i].equals("--fasta") ) {
                instance.createFastaFile = true;
            }
        }

        if( chr==null )
            instance.run(mapKey);
        else
            instance.run(mapKey, chr);

        instance.getLogWriter().close();

    }

    public void run(int mapKey) throws Exception {

        List<String> chromosomes = getChromosomes(mapKey);

        String fileNameBase = WORKING_DIR + "/" + mapKey;
        this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".log")));

        for( String chr: chromosomes ) {
            run(mapKey, chr);
        }
    }

    public void run(int mapKey, String chr) throws Exception {

        int species = MapManager.getInstance().getMap(mapKey).getSpeciesTypeKey();
        if(species != SpeciesType.RAT)
            simpleProteinQC = true;
        else simpleProteinQC = false;

        if( this.getLogWriter()==null ) {
            String fileNameBase = WORKING_DIR + "/" + mapKey + "." + chr;
            this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".log")));
        }

        String errorFileName = WORKING_DIR + "/ErrorFile_Assembly" + mapKey + "."+chr+".PolyPhen.error";
        errorFile = new BufferedWriter(new FileWriter(errorFileName));

        String polyphenFileName = WORKING_DIR + "/Assembly" + mapKey + "."+chr+".PolyPhenInput";
        this.getLogWriter().write("starting "+polyphenFileName+"\n");
        polyphenFile = new BufferedWriter(new FileWriter(polyphenFileName));
        polyphenFileInfo = new BufferedWriter(new FileWriter(polyphenFileName+".info"));
        polyphenFileInfo.append("#Note: if STRAND is '-', then inverted NUC_VAR is AA_REF\n");
        polyphenFileInfo.append("#VARIANT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID\n");


        if( createFastaFile ) {
            // create the fasta file
            String fastaFileName = WORKING_DIR + "/Assembly" + mapKey + "." + chr + ".PolyPhenInput.fasta";
            fastaFile = new BufferedWriter(new FileWriter(fastaFileName));
        }

        runAssembly( mapKey, chr);

        polyphenFileInfo.close();
        polyphenFile.close();
        errorFile.close();
        if( fastaFile!=null ) {
            fastaFile.close();
        }

        this.getLogWriter().write("finishing "+polyphenFileName+"\n\n\n");
    }

    public void runAssembly( int mapKey, String chr) throws Exception {

        int variantsProcessed = 0;
        int refSeqProteinLengthErrors = 0;
        int refSeqProteinLeftPartMismatch = 0;
        int refSeqProteinRightPartMismatch = 0;
        int proteinRefSeqNotInRgd = 0;
        int stopCodonsInProtein = 0;

        // /*+ INDEX(vt) */
        // this query hint forces oracle to use indexes on VARIANT_TRANSCRIPT table
        // (many times it was using full scans for unknown reasons)

       String sql = "SELECT /*+ INDEX(vt) */ \n" +
        "vm.start_pos, g.gene_symbol as region_name, t.gene_rgd_id, \n" +
        "v.ref_nuc, v.var_nuc, vt.ref_aa, vt.var_aa, vt.full_ref_aa_seq_key, vt.full_ref_aa_pos, \n" +
        "t.acc_id, t.protein_acc_id, vt.transcript_rgd_id, v.rgd_id\n" +
        "FROM variant v, variant_map_data vm, variant_transcript vt, transcripts t, genes g\n" +
        "WHERE vt.ref_aa <> vt.var_aa  AND  vt.var_aa<>'*' \n" +
        "AND v.ref_nuc IN ('A', 'G', 'C', 'T') \n" +
        "AND v.var_nuc IN ('A', 'G', 'C', 'T') \n" +
        "AND vt.ref_aa IS NOT NULL  AND  vt.var_aa IS NOT NULL \n" +
        "AND v.map_key = ? \n" +
        "AND v.chromosome = ? \n" +
        "AND v.rgd_id = vt.variant_rgd_id \n" +
        "AND v.rgd_id = vm.rgd_id \n" +
        "AND t.transcript_rgd_id=vt.transcript_rgd_id \n" +
        "AND t.gene_rgd_id=g.rgd_id";

        Connection conn = this.getVariantDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, mapKey);
        ps.setString(2, chr);
        ResultSet rs = ps.executeQuery();
        int lineNr = 0;
        String line;
        while( rs.next() ) {
            lineNr++;
            //long variantTranscriptId = rs.getLong(1);
            int startPos = rs.getInt(1);
            String regionName = rs.getString(2);
            //int geneRgdId = rs.getInt(4);
            //String refNuc = rs.getString(5);
            //String varNuc = rs.getString(6);
            String refAA = rs.getString(6);
            String varAA = rs.getString(7);
            int fullRefAASeqKey = rs.getInt(8);
            int fullRefAaaPos = rs.getInt(9);
            //String nucAccId = rs.getString(11);
            String proteinAccId = rs.getString(11);
            int transcriptRgdId = rs.getInt(12);
            long variantId = rs.getLong(13);

            String fullRefAA = null;
            if(fullRefAASeqKey != 0)
                fullRefAA = getfullRefAASequences(transcriptRgdId).get(0).getSeqData();
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

            if( simpleProteinQC ) {

                // simple protein QC:
                // there must not be any stop codons in the middle of the protein
                // only at the end, if any
                int stopCodonFirstPos = fullRefAA.indexOf('*');
                if( stopCodonFirstPos < fullRefAA.length()-1 ) {
                    // stop codons in the middle of the protein

                    // exception: if stop codons are 10 AAs after variant pos, that's OK
                    if( stopCodonFirstPos <= fullRefAaaPos+10 ) {
                        line = "stop codons in the middle of the protein sequence!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                                "    RefSeq protein length = " + fullRefAA.length() + "\n";
                        errorFile.append(line);
                        stopCodonsInProtein++;
                        this.getLogWriter().append("***STOP CODONS IN PROTEIN***\n" + line);
                        continue;
                    }
                }

                variantsProcessed++;

                String translatedLeftPart = fullRefAA.substring(0, fullRefAaaPos-1);

                // RefSeq protein part to the right of the mutation point must match the translated part
                String translatedRightPart = fullRefAA.substring(fullRefAaaPos);
                if( translatedRightPart.endsWith("*") )
                    translatedRightPart = translatedRightPart.substring(0, translatedRightPart.length()-1);

                this.getLogWriter().append("***MATCH***\n"+
                        "  proteinLeftPart\n" + translatedLeftPart+"\n"+
                        "  proteinRightPart\n" + translatedRightPart+"\n");

                // write polyphen input file
                // PROTEIN_ACC_ID POS REF_AA VAR_AA
                line = proteinAccId+" "+fullRefAaaPos+" "+refAA+" "+varAA+"\n";
                polyphenFile.write(line);

                // write polyphen input info file
                //#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID");
                line = variantId+"\t"+regionName+"\t"+proteinAccId+"\t"+fullRefAaaPos+"\t"+refAA+"\t"+varAA+"\t"+strand+"\t"+transcriptRgdId+"\n";
                polyphenFileInfo.write(line);

                writeFastaFile(proteinAccId, fullRefAA);
                continue;
            }

            // retrieve protein sequence from RGD
            List<Sequence> seqsInRgd = getProteinSequences(transcriptRgdId);
            if( seqsInRgd==null || seqsInRgd.isEmpty() ) {
                proteinRefSeqNotInRgd++;
                this.getLogWriter().append("***PROTEIN REFSEQ NOT IN RGD***\n");
            }
            else {
                for( Sequence seq: seqsInRgd ) {
                    variantsProcessed++;

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

                    // write polyphen input file
                    // PROTEIN_ACC_ID POS REF_AA VAR_AA
                    line = proteinAccId+" "+fullRefAaaPos+" "+refAA+" "+varAA+"\n";
                    polyphenFile.write(line);

                    // write polyphen input info file
                    //#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID");
                    line = variantId+"\t"+regionName+"\t"+proteinAccId+"\t"+fullRefAaaPos+"\t"+refAA+"\t"+varAA+"\t"+strand+"\t"+transcriptRgdId+"\n";
                    polyphenFileInfo.write(line);

                    writeFastaFile(proteinAccId, fullRefAA);
                }
            }
        }

        conn.close();

        getLogWriter().write("\nPROCESSING SUMMARY:");
        getLogWriter().write("\n  variantsProcessed = "+variantsProcessed);
        getLogWriter().write("\n  refSeqProteinLengthErrors = "+refSeqProteinLengthErrors);
        getLogWriter().write("\n  refSeqProteinLeftPartMismatch = "+refSeqProteinLeftPartMismatch);
        getLogWriter().write("\n  refSeqProteinRightPartMismatch = "+refSeqProteinRightPartMismatch);
        getLogWriter().write("\n  proteinRefSeqNotInRgd = "+proteinRefSeqNotInRgd);
        getLogWriter().write("\n  stopCodonsInProtein = "+stopCodonsInProtein);
        getLogWriter().newLine();
    }

    void writeFastaFile(String proteinAccId, String fullRefAA) throws IOException {
        if( fastaFile!=null ) {
            fastaFile.write(">"+proteinAccId);
            fastaFile.newLine();

            // write protein sequence, up to 70 characters per line
            for( int i=0; i<fullRefAA.length(); i+=70 ) {
                int chunkEndPos = i+70;
                if( chunkEndPos > fullRefAA.length() )
                    chunkEndPos = fullRefAA.length();
                String chunk = fullRefAA.substring(i, chunkEndPos);
                fastaFile.write(chunk);
                fastaFile.newLine();
            }
        }
    }

    List<Sequence> getProteinSequences(int transcriptRgdId) throws Exception {
        return sequenceDAO.getObjectSequences(transcriptRgdId, "ncbi_protein");
    }
    List<Sequence> getfullRefAASequences(int transcriptRgdId) throws Exception {
        return sequenceDAO.getObjectSequences(transcriptRgdId, "full_ref_aa");
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

    List<String> getChromosomes(int mapKey) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM variant_map_data WHERE map_key=? ";
        StringListQuery q = new StringListQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        return q.execute(new Object[]{mapKey});
    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
