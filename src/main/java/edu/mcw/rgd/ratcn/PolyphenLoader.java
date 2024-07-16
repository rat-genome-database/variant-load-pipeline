package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.CountQuery;
import edu.mcw.rgd.dao.spring.StringListQuery;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
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

    boolean VERIFY_IF_IN_RGD = false;
    public PolyphenLoader() throws Exception {

    }

    public static void main(String[] args) throws Exception {

//        XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        PolyphenLoader instance = (PolyphenLoader) (bf.getBean("polyphenLoader"));

        // process args
        int mapKey = 0;
        String chr = null;

        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--mapKey") ) {
                mapKey = Integer.parseInt(args[++i]);
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
            else if(args[i].equals("--verifyIfInRgd") || args[i].equals("-v")) {
                instance.VERIFY_IF_IN_RGD = true;
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

        String fileNameBase = getResultsDir() + "/" + mapKey;
        this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".load_results.log")));

        for( String chr: chromosomes ) {
            run(mapKey, chr);
        }
    }

    public void run(int mapKey, String chr) throws Exception {

        if( this.getLogWriter()==null ) {
            String fileNameBase = getResultsDir() + "/" + mapKey + "." + chr;
            this.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".load_results.log")));
        }

        // load info file for polyphen input
        // lines: #variant_id|variant_transcript_id|locus_name|protein_acc_id|pos|ref_aa|var_aa|strand|transcript_rgd_id
        // key: protein_acc_id|pos|ref_aa|var_aa
        List<String> infos = loadInfos(mapKey, chr);

        String resultFileName = getResultsDir() + "/" + mapKey + "." + chr + ".polyphen";
        BufferedReader resultFile = new BufferedReader(new FileReader(resultFileName));
        this.getLogWriter().write("opening file "+resultFileName+"\n");

        // skip header line
        resultFile.readLine();

        int linesProcessed = 0;
        int rowsInserted = 0;

        String line;
        while( (line=resultFile.readLine())!=null ) {

            PolyphenRecord polyphenRecord = new PolyphenRecord();
            String[] cols = line.split("[\\t]", -1);
//    0                      1      2       3      4               5                  6    7       8                                     11                      12             13                    14              15              16             17               18              19               20             21            22      23      24     25          26       27            28      29      30      31       32     33       34     35      36       37     38       39       40             41              42               43              44             45               46             47     48    49        50                  51           52              53               54
//#o_acc                   o_pos  o_aa1   o_aa2   rsid            acc                pos  aa1     aa2     nt1     nt2             prediction                  based_on        effect              pph2_class       pph2_prob        pph2_FPR        pph2_TPR        pph2_FDR          site          region            PHAT        dScore  Score1  Score2  MSAv      Nobs   Nstruct         Nfilt  PDB_id  PDB_pos PDB_ch   ident  length  NormASA SecStr  MapReg    dVol   dProp  B-fact   H-bonds         AveNHet         MinDHet         AveNInt         MinDInt         AveNSit         MinDSit        Transv  CodPos  CpG      MinDJxn             PfamHit      IdPmax          IdPSNP          IdQmin
//NP_001020045               197      D       N                   Q4TU74             197    D       N                                 benign                 alignment                               neutral               0               1               1           0.575            NO              NO                        -1.108  -2.700  -1.592     2         4                                                                                                                                                                                                                                                                            PF00046.24      25.673          25.673           81.06
//N

            polyphenRecord.setRefseqProteinAccId(cols[0].trim());
            polyphenRecord.setVarPos(Integer.parseInt(cols[1].trim())); // o_pos
            polyphenRecord.setoAA1(cols[2].trim()); // o_aa1
            polyphenRecord.setoAA2(cols[3].trim()); // o_aa2
            polyphenRecord.setUniprotAccId(cols[5].trim());
            polyphenRecord.setRefAA(cols[7].trim()); // aa1
            polyphenRecord.setVarAA(cols[8].trim()); // aa2
            polyphenRecord.setPrediction(cols[11].trim());
            polyphenRecord.setBasedOn(cols[12].trim());
            polyphenRecord.setEffect(cols[13].trim());
            polyphenRecord.setSite(cols[19].trim());
            polyphenRecord.setRegion(cols[20].trim());
            polyphenRecord.setPhat(cols[21].trim());
            polyphenRecord.setScore1(cols[23].trim());
            polyphenRecord.setScore2(cols[24].trim());
            polyphenRecord.setScoreDelta(cols[22].trim());
            polyphenRecord.setNumObserv(cols[26].trim());
            polyphenRecord.setNumStructInit(cols[27].trim());
            polyphenRecord.setNumStructFilt(cols[28].trim());
            polyphenRecord.setPdbId(cols[29].trim());
            polyphenRecord.setResNum(cols[30].trim());
            polyphenRecord.setChainId(cols[31].trim());
            polyphenRecord.setAliIde(cols[32].trim());
            polyphenRecord.setAliLen(cols[33].trim());
            polyphenRecord.setAccNormed(cols[34].trim());
            polyphenRecord.setSecStr(cols[35].trim());
            polyphenRecord.setMapRegion(cols[36].trim());
            polyphenRecord.setDeltaVolume(cols[37].trim());
            polyphenRecord.setDeltaProp(cols[38].trim());

            polyphenRecord.setbFact(cols[39].trim());
            polyphenRecord.setNumHBonds(cols[40].trim());
            polyphenRecord.setHetContAveNum(cols[41].trim());
            polyphenRecord.setHetContMinDist(cols[42].trim());
            polyphenRecord.setInterContAveNum(cols[43].trim());
            polyphenRecord.setInterContMinDist(cols[44].trim());
            polyphenRecord.setSitesContAveNum(cols[45].trim());
            polyphenRecord.setSitesContMinDist(cols[46].trim());

            polyphenRecord.setPph2Class(cols[14].trim());
            polyphenRecord.setPph2Prob(cols[15].trim());
            polyphenRecord.setPph2Fpr(cols[16].trim());
            polyphenRecord.setPph2Tpr(cols[17].trim());
            polyphenRecord.setPph2Fdr(cols[18].trim());
            polyphenRecord.setMsav(cols[25].trim());
            polyphenRecord.setTransv(cols[47].trim());
            polyphenRecord.setCpg(cols[49].trim());
            polyphenRecord.setMinDjxn(cols[50].trim());
            polyphenRecord.setPfamHit(cols[51].trim());
            polyphenRecord.setIdPmax(cols[52].trim());
            polyphenRecord.setIdPsnp(cols[53].trim());
            polyphenRecord.setIdQmin(cols[54].trim());
            polyphenRecord.setCodPos(cols[48].trim());

            // if o_aa1,o_aa2 is swapped with aa1,aa2, that means a problem: transcript sequence as extracted
            //   from reference chromosome fasta file differs from transcript nucleotide sequence
            // that results in variant/reference AAs different than the ones computed by carpe pipeline
            // so polyphen predictions are out of place; they should not be loaded
            if( polyphenRecord.getoAA1().equals(polyphenRecord.getVarAA()) && polyphenRecord.getoAA2().equals(polyphenRecord.getRefAA()) ) {
                // it should be oAA1==refAA && oAA2==varAA
                System.out.println("ERROR polyphen swapped AA resides "+polyphenRecord.getRefseqProteinAccId()+"|"+polyphenRecord.getVarPos()+"|"+
                        polyphenRecord.getoAA1()+"|"+polyphenRecord.getoAA2());
                continue;
            }

            String infoLine = extractInfo(polyphenRecord.getRefseqProteinAccId(),polyphenRecord.getVarPos(),polyphenRecord.getoAA1(),polyphenRecord.getoAA2(), infos);
            if( infoLine==null ) {
                System.out.println("ERROR matching polyphen result with info file "+polyphenRecord.getRefseqProteinAccId()+"|"+polyphenRecord.getVarPos()+"|"+polyphenRecord.getoAA1()+"|"+polyphenRecord.getoAA2());
                continue;
            }
            String[] infoCols = infoLine.split("[\\t]", -1);
            polyphenRecord.setVariantId(Long.parseLong(infoCols[0]));
            polyphenRecord.setGeneSymbol(infoCols[1]);
            polyphenRecord.setStrand(infoCols[6]);
            //polyphenRecord.setVariantTranscriptId(Long.parseLong(infoCols[1]));
            polyphenRecord.setProteinStatus("100 PERC MATCH");

            if( infoCols.length>7 )
                polyphenRecord.setTranscriptRgdId(Integer.parseInt(infoCols[7]));

            if( insertToBatch(polyphenRecord) ) {
                rowsInserted++;
            }

            linesProcessed++;
        }

        flushBatch();
        this.getLogWriter().write("closing file "+resultFileName+"\n");
        this.getLogWriter().write(rowsInserted+" polyphen results were loaded into db for assembly "+mapKey+" and chromosome "+chr+"\n");
        this.getLogWriter().write(linesProcessed+" polyphen results were processed for assembly "+mapKey+" and chromosome "+chr+"\n\n");

        resultFile.close();
    }

    List<PolyphenRecord> polyBatch = new ArrayList<PolyphenRecord>();

    boolean insertToBatch( PolyphenRecord p ) throws Exception{

        if(VERIFY_IF_IN_RGD) {
            String polyphenSql = "SELECT COUNT(*) FROM polyphen WHERE variant_rgd_id=? AND protein_id=? AND position=? " +
                    " AND aa1=? AND aa2=? AND uniprot_acc=? AND transcript_rgd_id=? AND o_aa1=? AND o_aa2=?";
            CountQuery q = new CountQuery(this.getDataSource(), polyphenSql);
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.INTEGER));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.INTEGER));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            q.declareParameter(new SqlParameter(Types.VARCHAR));
            int cnt = q.getCount(new Object[]{p.getVariantId(), p.getRefseqProteinAccId(), p.getVarPos(), p.getRefAA(), p.getVarAA(),
                    p.getUniprotAccId(), p.getTranscriptRgdId(), p.getoAA1(), p.getoAA2()});
            if (cnt != 0) {
                // this information is already in polyphen table; nothing more to do (we do not want insert duplicates)
                return false;
            }
        }
        polyBatch.add(p);
        if( polyBatch.size()==10000 )
            flushBatch();
        return true;
    }
    void flushBatch() throws Exception{

        insert(polyBatch);

        polyBatch.clear();
    }
    boolean insert(List<PolyphenRecord> polyphenRecordList) throws Exception {


String polyphenSql = "INSERT INTO polyphen (polyphen_id,variant_rgd_id,gene_symbol,protein_id,position,aa1,aa2, "+
                "prediction, basis, effect, site, region, phat, score1, score2, score_delta, num_observ, num_struct_init, "+
                "num_struct_filt, pdb_id, res_num, chain_id, ali_ide, ali_len, acc_normed, sec_str, map_region, "+
                "delta_volume, delta_prop, b_fact, num_h_bonds, het_cont_ave_num, het_cont_min_dist, inter_cont_ave_num, "+
                "inter_cont_min_dist, sites_cont_ave_num, sites_cont_min_dist, uniprot_acc, inverted_flag, "+
                "transcript_rgd_id, protein_status, o_aa1, o_aa2, pph2_class, pph2_prob, "+
                "pph2_fpr, pph2_tpr, pph2_fdr, msav, transv, cpg, min_djxn, pfam_hit, id_pmax, id_psnp, id_qmin," +
                "cod_pos, creation_date) "+
                "VALUES(POLYPHEN_SEQ.NEXTVAL,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,"+
                "?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?, ?,SYSDATE)";
        BatchSqlUpdate su = new BatchSqlUpdate(this.getVariantDataSource(), polyphenSql, new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR},10000);

        su.compile();
        for(PolyphenRecord p: polyphenRecordList) {
            su.update(p.getVariantId(), p.getGeneSymbol(), p.getRefseqProteinAccId(), p.getVarPos(),p.getRefAA(), p.getVarAA(), p.getPrediction(),
                    p.getBasedOn(), p.getEffect(), p.getSite(), p.getRegion(), p.getPhat(),p.getScore1(), p.getScore2(), p.getScoreDelta(),p.getNumObserv(),
                    p.getNumStructInit(), p.getNumStructFilt(), p.getPdbId(), p.getResNum(), p.getChainId(), p.getAliIde(), p.getAliLen(),p.getAccNormed(),
                    p.getSecStr(), p.getMapRegion(), p.getDeltaVolume(), p.getDeltaProp(), p.getbFact(), p.getNumHBonds(), p.getHetContAveNum(), p.getHetContMinDist(),
                    p.getInterContAveNum(), p.getInterContMinDist(), p.getSitesContAveNum(), p.getSitesContMinDist(), p.getUniprotAccId(), p.getStrand(),
                    p.getTranscriptRgdId(), p.getProteinStatus(), p.getoAA1(), p.getoAA2(), p.getPph2Class(), p.getPph2Prob(),
                    p.getPph2Fpr(), p.getPph2Tpr(),p.getPph2Fdr(),p.getMsav(),p.getTransv(), p.getCpg(), p.getMinDjxn(), p.getPfamHit(), p.getIdPmax(),
                    p.getIdPsnp(), p.getIdQmin(), p.getCodPos());

        }

        su.flush();
       /* String vartrSql = "UPDATE "+varTrTable+" SET polyphen_status=?,uniprot_id=?,protein_id=? "+
                "WHERE variant_transcript_id=?";
        su = new BatchSqlUpdate(this.getDataSource(), vartrSql, new int[]{Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER},10000);
        for(PolyphenRecord p: polyphenRecordList) {
            su.update(p.getPrediction(), p.getUniprotAccId(), p.getRefseqProteinAccId(), p.getVariantTranscriptId());
        }
        su.flush();
        */
        return true;
    }

    // load info file for polyphen input
    // lines: variant_id|variant_transcript_id|locus_name|protein_acc_id|pos|ref_aa|var_aa|strand|transcript_rgd_id
    List<String> loadInfos(int mapKey, String chr) throws Exception {

        List<String> infos = new ArrayList<String>();

        String infoFileName = getOutputDir() + "/Assembly" + mapKey + "." + chr + ".PolyPhenInput.info";
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
            if( !cols[2].equals(proteinAccId) )
                continue;
            if( !cols[3].equals(pos) )
                continue;
            if( !cols[4].equals(refAA) )
                continue;
            if( !cols[5].equals(varAA) )
                continue;

            it.remove();
            return line;
        }

        return null;
    }

    List<String> getChromosomes(int mapKey) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM variant_map_data WHERE map_key=? ";
        StringListQuery q = new StringListQuery(getDataSource(), sql);
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
