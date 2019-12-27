package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.CountQuery;
import edu.mcw.rgd.dao.spring.StringListQuery;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.SqlUpdate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 12/30/13
 * loads polyphen results back into database
 */
public class PolyphenLoader extends VariantProcessingBase {

    private String version;
    private String resultsDir;
    private String outputDir;

    String varTable = "VARIANT";
    String varTrTable = "VARIANT_TRANSCRIPT";
    String polyTable = "POLYPHEN";
    public PolyphenLoader() throws Exception {

    }

    public static void main(String[] args) throws Exception {

        XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        PolyphenLoader instance = (PolyphenLoader) (bf.getBean("polyphenLoader"));

        // process args
        int sampleId = 0;
        String chr = null;

        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--sample") ) {
                sampleId = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("--chr") ) {
                chr = args[++i];
            }
            else if( args[i].equals("--resultsDir") ) {
                instance.setResultsDir(args[++i]);
            }
            else if( args[i].equals("--outputDir") ) {
                instance.setOutputDir(args[++i]);
            }
        }

        if( chr==null )
            instance.run(sampleId);
        else
            instance.run(sampleId, chr);

        instance.getLogWriter().close();
    }

    public void run(int sampleId) throws Exception {

        VariantDAO vdao = new VariantDAO();
        varTable = vdao.getVariantTable(sampleId);
        varTrTable = vdao.getVariantTranscriptTable(sampleId);
        polyTable = vdao.getPolyphenTable(sampleId);

        List<String> chromosomes = getChromosomes(sampleId);

        String fileNameBase = getResultsDir() + "/" + sampleId;
        this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".load_results.log")));

        for( String chr: chromosomes ) {
            run(sampleId, chr);
        }
    }

    public void run(int sampleId, String chr) throws Exception {

        VariantDAO vdao = new VariantDAO();
        varTable = vdao.getVariantTable(sampleId);
        varTrTable = vdao.getVariantTranscriptTable(sampleId);
        polyTable = vdao.getPolyphenTable(sampleId);

        if( this.getLogWriter()==null ) {
            String fileNameBase = getResultsDir() + "/" + sampleId + "." + chr;
            this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".load_results.log")));
        }

        // load info file for polyphen input
        // lines: #variant_id|variant_transcript_id|locus_name|protein_acc_id|pos|ref_aa|var_aa|strand|transcript_rgd_id
        // key: protein_acc_id|pos|ref_aa|var_aa
        List<String> infos = loadInfos(sampleId, chr);

        String resultFileName = getResultsDir() + "/" + sampleId + "." + chr + ".polyphen";
        BufferedReader resultFile = new BufferedReader(new FileReader(resultFileName));
        this.getLogWriter().write("opening file "+resultFileName+"\n");

        // skip header line
        resultFile.readLine();

        int linesProcessed = 0;
        int rowsInserted = 0;

        String line;
        while( (line=resultFile.readLine())!=null ) {

            String[] cols = line.split("[\\t]", -1);
//    0                      1      2       3      4               5                  6    7       8                                     11                      12             13                    14              15              16             17               18              19               20             21            22      23      24     25          26       27            28      29      30      31       32     33       34     35      36       37     38       39       40             41              42               43              44             45               46             47     48    49        50                  51           52              53               54
//#o_acc                   o_pos  o_aa1   o_aa2   rsid            acc                pos  aa1     aa2     nt1     nt2             prediction                  based_on        effect              pph2_class       pph2_prob        pph2_FPR        pph2_TPR        pph2_FDR          site          region            PHAT        dScore  Score1  Score2  MSAv      Nobs   Nstruct         Nfilt  PDB_id  PDB_pos PDB_ch   ident  length  NormASA SecStr  MapReg    dVol   dProp  B-fact   H-bonds         AveNHet         MinDHet         AveNInt         MinDInt         AveNSit         MinDSit        Transv  CodPos  CpG      MinDJxn             PfamHit      IdPmax          IdPSNP          IdQmin
//NP_001020045               197      D       N                   Q4TU74             197    D       N                                 benign                 alignment                               neutral               0               1               1           0.575            NO              NO                        -1.108  -2.700  -1.592     2         4                                                                                                                                                                                                                                                                            PF00046.24      25.673          25.673           81.06
//N

            String refseqProteinAccId = cols[0].trim();
            int varPos = Integer.parseInt(cols[1].trim()); // o_pos
            String oAA1 = cols[2].trim(); // o_aa1
            String oAA2 = cols[3].trim(); // o_aa2
            String uniprotAccId = cols[5].trim();
            String refAA = cols[7].trim(); // aa1
            String varAA = cols[8].trim(); // aa2
            String prediction = cols[11].trim();
            String basedOn = cols[12].trim();
            String effect = cols[13].trim();
            String site = cols[19].trim();
            String region = cols[20].trim();
            String phat = cols[21].trim();
            String score1 = cols[23].trim();
            String score2 = cols[24].trim();
            String scoreDelta = cols[22].trim();
            String numObserv = cols[26].trim();
            String numStructInit = cols[27].trim();
            String numStructFilt = cols[28].trim();
            String pdbId = cols[29].trim();
            String resNum = cols[30].trim();
            String chainId = cols[31].trim();
            String aliIde = cols[32].trim();
            String aliLen = cols[33].trim();
            String accNormed = cols[34].trim();
            String secStr = cols[35].trim();
            String mapRegion = cols[36].trim();
            String deltaVolume = cols[37].trim();
            String deltaProp = cols[38].trim();

            String bFact = cols[39].trim();
            String numHBonds = cols[40].trim();
            String hetContAveNum = cols[41].trim();
            String hetContMinDist = cols[42].trim();
            String interContAveNum = cols[43].trim();
            String interContMinDist = cols[44].trim();
            String sitesContAveNum = cols[45].trim();
            String sitesContMinDist = cols[46].trim();

            String pph2Class = cols[14].trim();
            String pph2Prob = cols[15].trim();
            String pph2Fpr = cols[16].trim();
            String pph2Tpr = cols[17].trim();
            String pph2Fdr = cols[18].trim();
            String msav = cols[25].trim();
            String transv = cols[47].trim();
            String cpg = cols[49].trim();
            String minDjxn = cols[50].trim();
            String pfamHit = cols[51].trim();
            String idPmax = cols[52].trim();
            String idPsnp = cols[53].trim();
            String idQmin = cols[54].trim();
            String codPos = cols[48].trim();

            // if o_aa1,o_aa2 is swapped with aa1,aa2, that means a problem: transcript sequence as extracted
            //   from reference chromosome fasta file differs from transcript nucleotide sequence
            // that results in variant/reference AAs different than the ones computed by carpe pipeline
            // so polyphen predictions are out of place; they should not be loaded
            if( oAA1.equals(varAA) && oAA2.equals(refAA) ) {
                // it should be oAA1==refAA && oAA2==varAA
                System.out.println("ERROR polyphen swapped AA resides "+refseqProteinAccId+"|"+varPos+"|"+oAA1+"|"+oAA2);
                continue;
            }

            String infoLine = extractInfo(refseqProteinAccId, varPos, oAA1, oAA2, infos);
            if( infoLine==null ) {
                System.out.println("ERROR matching polyphen result with info file "+refseqProteinAccId+"|"+varPos+"|"+oAA1+"|"+oAA2);
                continue;
            }
            String[] infoCols = infoLine.split("[\\t]", -1);
            long variantId = Long.parseLong(infoCols[0]);
            String geneSymbol = infoCols[2];
            String strand = infoCols[7];
            long variantTranscriptId = Long.parseLong(infoCols[1]);
            String proteinStatus = "100 PERC MATCH";
            Integer transcriptRgdId = null;
            if( infoCols.length>8 )
                transcriptRgdId = Integer.parseInt(infoCols[8]);

            if( insert(variantId, geneSymbol, refseqProteinAccId, varPos, refAA, varAA, prediction, basedOn, effect,
                site, region, phat, score1, score2, scoreDelta, numObserv, numStructInit, numStructFilt, pdbId,
                resNum, chainId, aliIde, aliLen, accNormed, secStr, mapRegion, deltaVolume, deltaProp, bFact,
                numHBonds, hetContAveNum, hetContMinDist, interContAveNum, interContMinDist, sitesContAveNum,
                sitesContMinDist, uniprotAccId, strand, transcriptRgdId, variantTranscriptId, proteinStatus,
                oAA1, oAA2, pph2Class, pph2Prob, pph2Fpr, pph2Tpr, pph2Fdr, msav, transv, cpg, minDjxn, pfamHit,
                idPmax, idPsnp, idQmin, codPos) ) {
                rowsInserted++;
            }

            linesProcessed++;
        }
        this.getLogWriter().write("closing file "+resultFileName+"\n");
        this.getLogWriter().write(rowsInserted+" polyphen results were loaded into db for sample "+sampleId+" and chromosome "+chr+"\n");
        this.getLogWriter().write(linesProcessed+" polyphen results were processed for sample "+sampleId+" and chromosome "+chr+"\n\n");

        resultFile.close();
    }

    boolean insert(long variantId, String geneSymbol, String proteinId, int position, String aa1, String aa2,
        String prediction, String basis, String effect, String site, String region, String phat, String score1,
        String score2, String scoreDelta, String numObserv, String numStructInit, String numStructFilt,
        String pdbId, String resNum, String chainId, String aliIde, String aliLen, String accNormed,
        String secStr, String mapRegion, String deltaVolume, String deltaProp, String bFact, String numHBonds,
        String hetContAveNum, String hetContMinDist, String interContAveNum, String interContMinDist,
        String sitesContAveNum, String sitesContMinDist, String uniprotAcc, String invertedFlag,
        Integer transcriptRgdId, long variantTranscriptId, String proteinStatus, String oAA1, String oAA2,
        String pph2Class, String pph2Prob, String pph2Fpr, String pph2Tpr, String pph2Fdr, String msav,
        String transv, String cpg, String minDjxn, String pfamHit, String idPmax, String idPsnp, String idQmin,
        String codPos) throws Exception {

        String polyphenSql = "SELECT COUNT(*) FROM "+polyTable+" WHERE variant_id=? AND protein_id=? AND position=? "+
                " AND aa1=? AND aa2=? AND uniprot_acc=? AND transcript_rgd_id=? AND variant_transcript_id=? AND o_aa1=? AND o_aa2=?";
        CountQuery q = new CountQuery(this.getDataSource(), polyphenSql);
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        int cnt = q.getCount(new Object[]{variantId, proteinId, position, aa1, aa2, uniprotAcc, transcriptRgdId, variantTranscriptId, oAA1, oAA2});
        if( cnt!=0 ) {
            // this information is already in polyphen table; nothing more to do (we do not want insert duplicates)
            return false;
        }

        polyphenSql = "INSERT INTO "+polyTable+" (polyphen_id,variant_id,gene_symbol,protein_id,position,aa1,aa2, "+
                "prediction, basis, effect, site, region, phat, score1, score2, score_delta, num_observ, num_struct_init, "+
                "num_struct_filt, pdb_id, res_num, chain_id, ali_ide, ali_len, acc_normed, sec_str, map_region, "+
                "delta_volume, delta_prop, b_fact, num_h_bonds, het_cont_ave_num, het_cont_min_dist, inter_cont_ave_num, "+
                "inter_cont_min_dist, sites_cont_ave_num, sites_cont_min_dist, uniprot_acc, inverted_flag, "+
                "transcript_rgd_id, variant_transcript_id, protein_status, o_aa1, o_aa2, pph2_class, pph2_prob, "+
                "pph2_fpr, pph2_tpr, pph2_fdr, msav, transv, cpg, min_djxn, pfam_hit, id_pmax, id_psnp, id_qmin," +
                "cod_pos, creation_date) "+
                "VALUES(POLYPHEN_SEQ.NEXTVAL,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,"+
                "?,?,?,?,?, ?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?, ?,SYSDATE)";
        SqlUpdate su = new SqlUpdate(this.getDataSource(), polyphenSql, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
        su.update(variantId, geneSymbol, proteinId, position, aa1, aa2, prediction, basis, effect, site, region, phat,
            score1, score2, scoreDelta, numObserv, numStructInit, numStructFilt, pdbId, resNum, chainId, aliIde, aliLen,
            accNormed, secStr, mapRegion, deltaVolume, deltaProp, bFact, numHBonds, hetContAveNum, hetContMinDist,
            interContAveNum, interContMinDist, sitesContAveNum, sitesContMinDist, uniprotAcc, invertedFlag,
            transcriptRgdId, variantTranscriptId, proteinStatus, oAA1, oAA2, pph2Class, pph2Prob, pph2Fpr, pph2Tpr,
            pph2Fdr, msav, transv, cpg, minDjxn, pfamHit, idPmax, idPsnp, idQmin, codPos);


        String vartrSql = "UPDATE "+varTrTable+" SET polyphen_status=?,uniprot_id=?,protein_id=? "+
                "WHERE variant_transcript_id=?";
        su = new SqlUpdate(this.getDataSource(), vartrSql, new int[]{Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.INTEGER});
        su.update(prediction, uniprotAcc, proteinId, variantTranscriptId);

        return true;
    }

    // load info file for polyphen input
    // lines: variant_id|variant_transcript_id|locus_name|protein_acc_id|pos|ref_aa|var_aa|strand|transcript_rgd_id
    List<String> loadInfos(int sampleId, String chr) throws Exception {

        List<String> infos = new ArrayList<String>();

        String infoFileName = getOutputDir() + "/Sample" + sampleId + "." + chr + ".PolyPhenInput.info";
        BufferedReader infoFile = new BufferedReader(new FileReader(infoFileName));
        this.getLogWriter().write("opening file " + infoFileName + "\n");

        String line;
        while( (line=infoFile.readLine())!=null ) {

            // skip comment lines
            if( line.startsWith("#") )
                continue;
            infos.add(line);
        }

        this.getLogWriter().write("closing file "+infoFileName+"\n\n");
        infoFile.close();

        return infos;
    }

    String extractInfo(String proteinAccId, int varPos, String refAA, String varAA, List<String>infos) {

        String pos = Integer.toString(varPos);

        Iterator<String> it = infos.iterator();
        while( it.hasNext() ) {
            String line = it.next();
            String[] cols = line.split("[\\t]", -1);
            if( !cols[3].equals(proteinAccId) )
                continue;
            if( !cols[4].equals(pos) )
                continue;
            if( !cols[5].equals(refAA) )
                continue;
            if( !cols[6].equals(varAA) )
                continue;

            it.remove();
            return line;
        }

        return null;
    }

    List<String> getChromosomes(int sampleId) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM "+varTable+" WHERE sample_id=? ";
        StringListQuery q = new StringListQuery(getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        return q.execute(new Object[]{sampleId});
    }



    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public String getResultsDir() {
        return resultsDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }
}
