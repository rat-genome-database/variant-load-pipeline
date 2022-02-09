package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.dao.impl.AssociationDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.dao.spring.XmlBeanFactoryManager;
import edu.mcw.rgd.datamodel.Association;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.ratcn.VariantPostProcessing;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mtutaj
 * @since 5/21/14
 * TODO
 * <ul>
 *     <li>cross-compare ClinVar positions on assembly 37 vs positions derived from gene symbol and preferred name</li>
 *     <li>cross-compare ClinVar positions on assembly 37 vs position read from hgvs name</li>
 * </ul>
 */
public class ClinVar2Vcf {

    public static void main(String[] args) throws Exception {

        List<Integer> mapKeys = new ArrayList<>();
        List<String> outputFiles = new ArrayList<>();

        for( int i=0; i<args.length; i++ ) {
            switch( args[i] ) {
                case "--mapKey":
                    mapKeys.add(Integer.parseInt(args[++i]));
                    break;

                case "--outputFile":
                    outputFiles.add(args[++i]);
                    break;
            }
        }

        ClinVar2Vcf converter = new ClinVar2Vcf();
        converter.loadVarRgdId2RsIdMap();

        for( int i=0; i<mapKeys.size(); i++ ) {
            converter.run(mapKeys.get(i), outputFiles.get(i));
        }

        System.out.println("---OK---");
    }

    static final int refCount = 8;
    static final int varCount = 1;

    int noStrandVariants;
    int plusStrandVariants;
    int minusStrandVariants;
    int linesWritten;
    int linesWithRsId;
    Map<Integer, String> varRgdId2RsId = new HashMap<>();

    void run(int mapKey, String outputFile) throws Exception {

        System.out.println("CLINVAR2VCF started");
        System.out.println("  --mapKey     "+mapKey);
        System.out.println("  --outputFile "+outputFile);

        BufferedWriter writer = Utils.openWriter(outputFile);
        writer.write("##fileformat=VCFv4.1\n");
        writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tClinVar"+mapKey+"\n");

        int linesProcessed = 0;
        int linesWithIssues = 0;
        int hgvsNameUsedAsPreferredName = 0;

        Connection conn = getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT v.rgd_id,name,object_type \n" +
            "FROM clinvar v,genomic_elements ge\n" +
            "WHERE v.rgd_id=ge.rgd_id \n" +
            "AND object_type in('single nucleotide variant','deletion','insertion','duplication') " +
            "order by dbms_random.value");
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            Record r = new Record();
            r.varRgdId = rs.getInt(1);
            // variant must have a position on human assembly
            if( !getVarPos(r, mapKey) ) {
                continue;
            }
            linesProcessed++;
            r.pos = linesProcessed;

            r.name = rs.getString(2);
            r.type = rs.getString(3);
            if( !processPreferredName(r, mapKey) ) {
                String origName = r.name;
                r.name = loadPrimaryHgvsName(r);
                if( !processPreferredName(r, mapKey) ) {
                    System.out.println("unsupported format of preferred name ["+origName+"], hgvs-name ["+r.name+"], rgd_id="+r.varRgdId);
                    linesWithIssues++;
                    continue;
                }
                else
                    hgvsNameUsedAsPreferredName++;
            }

            if( !getRefAndVarNuc(r) ) {
                linesWithIssues++;
                continue;
            }

            if( r.varStopPos-r.varStartPos+1 != r.refNuc.length() ) {
                System.out.println("ClinVar position inconsistency "+r.geneSymbol+" varRgdId="+r.varRgdId);
                linesWithIssues++;
                continue;
            }

            if( !qcVarNucAndRefNuc(r) ) {
                System.out.println("bad varNuc or refNuc "+r.geneSymbol+" varRgdId="+r.varRgdId);
                linesWithIssues++;
                continue;
            }

            handleStrandedness(r);

            writeVcfLine(r, writer);
        }
        conn.close();

        writer.close();

        System.out.println("linesProcessed    ="+linesProcessed);
        System.out.println("  linesWritten ="+linesWritten);
        System.out.println("  linesWithIssues ="+linesWithIssues);
        System.out.println("  hgvsNameUsedAsPreferredName ="+hgvsNameUsedAsPreferredName);
        System.out.println("  linesWithPlusStrand ="+plusStrandVariants);
        System.out.println("  linesWithMinusStrand ="+minusStrandVariants);
        System.out.println("  linesWithNoStrand ="+noStrandVariants);
        System.out.println("  linesWithRsId ="+linesWithRsId);
    }

    boolean processPreferredName(Record r, int mapKey) throws Exception {
        r.chr = null;
        r.geneStartPos = 0; // as read from RGD db

        // get rid of parentheses (and extract protein change)
        r.name2 = extractProteinChange(r);
        if( r.name2==null )
            return false;

        // extract gene name
        int pos1 = r.name2.indexOf(":c.");
        if( pos1>0 ) {
            r.geneSymbol = r.name2.substring(0, pos1);
            //r.geneRgdId = getGeneRgdId(r.geneSymbol);
            r.geneRgdId = getGeneRgdId(r.varRgdId);
            getGenePos(r);
            extractLocusAndNucChange(r, pos1+3);
        } else {
            // genomic position, like NC_000005.10:g.171328520C>T
            pos1 = r.name2.indexOf(":g.");
            if( pos1>0 ) {
                r.ncAccId = r.name2.substring(0, pos1);
                getTrPos(r, mapKey);
                extractLocusAndNucChange(r, pos1+3);
            } else {
                return false;
            }
        }
        return true;
    }

    boolean qcVarNucAndRefNuc(Record r) {
        // varNuc and refNuc must be composed entirely from characters 'ACGT-'
        if( r.refNuc==null || r.refNuc.isEmpty() )
            return false;
        if( r.varNuc==null || r.varNuc.isEmpty() )
            return false;

        if( !r.refNuc.equals("-") ) {
            // refnuc must be a combination of ACGT
            for( int i=0; i<r.refNuc.length(); i++ ) {
                char c = r.refNuc.charAt(i);
                if( c!='A' && c!='C' && c!='G' && c!='T' ) {
                    System.out.println("   invalid refNuc "+r.refNuc);
                    return false;
                }
            }
        }

        if( !r.varNuc.equals("-") ) {
            // varnuc must be a combination of ACGT
            for( int i=0; i<r.varNuc.length(); i++ ) {
                char c = r.varNuc.charAt(i);
                if( c!='A' && c!='C' && c!='G' && c!='T' ) {
                    System.out.println("   invalid varNuc "+r.varNuc);
                    return false;
                }
            }
        }

        return true;
    }

    void handleStrandedness(Record r) throws Exception {
        if( r.strand==null ) {
            if( r.ncAccId!=null && !r.ncAccId.startsWith("NC") && !r.ncAccId.startsWith("NG") ) {
                System.out.println("NO STRAND "+r.name+" varRgdId="+r.varRgdId);
            }
            noStrandVariants++;
        } else if( r.strand.equals("+")) {
            plusStrandVariants++;
        } else if( r.strand.equals("-") ) {
            minusStrandVariants++;

            // convert refNuc and varNuc to negative strand
            if( !r.varNuc.equals("-") ) {
                r.varNuc = VariantPostProcessing.reverseComplement(r.varNuc).toString();
            }
            if( !r.refNuc.equals("-") ) {
                r.refNuc = VariantPostProcessing.reverseComplement(r.refNuc).toString();
            }
        } else {
            throw new Exception("unexpected strand "+r.strand);
        }
    }

    void writeVcfLine(Record r, BufferedWriter writer) throws IOException {

        // CHROM
        writer.write(r.varChr);

        // POS
        writer.write("\t" + r.varStartPos);

        // ID
        writer.write("\t" + "RGDID:"+r.varRgdId+";"+r.name);

        // REF
        writer.write("\t" + r.refNuc);

        // ALT
        writer.write("\t" + r.varNuc);

        // QUAL
        writer.write("\tPASS");

        // FILTER
        writer.write("\tVALIDATED=1");

        // INFO
        String rsId = varRgdId2RsId.get(r.varRgdId);
        if( rsId==null ) { // no rs id
            writer.write("\t");
        } else {
            writer.write("DB:"+rsId+"\t");
            linesWithRsId++;
        }

        // FORMAT
        writer.write("\tGT;AD;DP");

        writer.write("\t0/1:"+refCount+","+varCount+":"+(refCount+varCount));

        writer.write("\n");

        linesWritten++;
    }

    boolean getRefAndVarNuc(Record r) {
        if( r.type.equals("single nucleotide variant") ) {
            // find '>'
            int pos = r.nucChange.indexOf('>');
            if( pos>0 ) {
                r.refNuc = r.nucChange.substring(0, pos);
                r.varNuc = r.nucChange.substring(pos+1);
                return true;
            } else {
                pos = r.nucChange.indexOf('=');
                if( pos>0 ) {
                    r.refNuc = r.nucChange.substring(0, pos);
                    r.varNuc = r.refNuc;
                    return true;
                } else {
                    System.out.println(" unsupported snv: "+r.name+" varRgdId="+r.varRgdId);
                    return false;
                }
            }
        }

        if( r.type.equals("deletion") ) {
                // fe 'delCT'
            if( r.nucChange.startsWith("del") ) {
                r.refNuc = r.nucChange.substring(3);
                r.varNuc = "-";
                return true;
            } else {
                System.out.println(" unsupported del: "+r.name+" varRgdId="+r.varRgdId);
                return false;
            }
        }

        if( r.type.equals("duplication") ) {
                // fe 'dupAAACCGCCCC'
            if( r.nucChange.startsWith("dup") ) {
                r.refNuc = r.nucChange.substring(3);
                r.varNuc = r.refNuc+r.refNuc;
                return true;
            }
            else if( r.nucChange.startsWith("ins") ) {
                r.refNuc = r.nucChange.substring(3);
                r.varNuc = r.refNuc+r.refNuc;
                r.varStopPos--; // customarily (HGVS names) for insertion, positions for start and end nucleotides are provided
                // fe c.51_52insT is a single nucl insertion between 51st and 52nd nucleotides of CDS
                // we decrease stop pos to make our QC code work
                return true;
            } else {
                System.out.println(" unsupported dup: "+r.name+" varRgdId="+r.varRgdId);
                return false;
            }
        }

        if( r.type.equals("insertion") ) {
                // fe 'insT'
            if( r.nucChange.startsWith("ins") ) {
                r.refNuc = "-";
                r.varNuc = r.nucChange.substring(3);
                r.varStopPos--; // customarily (HGVS names) for insertion, positions for start and end nucleotides are provided
                // fe c.51_52insT is a single nucl insertion between 51st and 52nd nucleotides of CDS
                // we decrease stop pos to make our QC code work
                return true;
            } else {
                System.out.println(" unsupported ins: "+r.name+" varRgdId="+r.varRgdId);
                return false;
            }
        }

        System.out.println("unsupported var type");
        return true;
    }

    void extractLocusAndNucChange(Record r, int pos) {

        // parse name2, such as '749C>T' into locus='749' and nucChange='C>T'
        //
        // stop at 1st non-digit character
        int i;
        for( i=pos; i<r.name2.length(); i++ ) {
            char c = r.name2.charAt(i);
            if( !Character.isDigit(c)  &&  c!='+'  &&  c!='-'  &&  c!='_'  &&  c!='*' )
                break;
        }
        r.locus = r.name2.substring(pos, i);
        r.nucChange = r.name2.substring(i);

        if( r.ncAccId!=null ) {
            // absolute genomic pos given
            //r.pos = r.varStartPos;
        } else {

        }
    }

    String extractProteinChange(Record r) {
        if( r.name==null )
            return null;

        int pos1 = r.name.lastIndexOf("(p.");
        int pos2 = r.name.lastIndexOf(")");
        if( pos1>0 && pos1<pos2 ) {
            r.proteinChange = r.name.substring(pos1+1, pos2);
            return r.name.substring(0, pos1).trim();
        }
        else {
            return r.name;
        }
    }

    int getGeneRgdId(int variantRgdId) throws Exception {

        AssociationDAO associationDAO = new AssociationDAO();
        List<Association> assocs = associationDAO.getAssociationsForMasterRgdId(variantRgdId, "variant_to_gene");
        if( assocs.isEmpty() ) {
            System.out.println("No gene assocs for VAR_RGD_ID="+variantRgdId);
            return 0;
        }
        if( assocs.size()>1 ) {
            System.out.println("Multiple genes for VAR_RGD_ID="+variantRgdId);
            return 0;
        }
        return assocs.get(0).getDetailRgdId();
    }

    void getGenePos(Record r) throws SQLException {

        r.chr = null;

        Connection conn = getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
                "SELECT md.start_pos, md.stop_pos,md.chromosome,md.strand FROM maps_data md WHERE md.rgd_id=? AND map_key=?");
        ps.setInt(1, r.geneRgdId);
        ps.setInt(2, 17);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            if( r.chr!=null ) {
                System.out.println("multiple positions for "+r.geneSymbol);
            } else {
                r.geneStartPos = rs.getInt(1);
                r.chr = rs.getString(3);
                r.strand = rs.getString(4);
            }
        }
        conn.close();
    }

    // get position of transcript
    void getTrPos(Record r, int mapKey) throws Exception {
        if( r.ncAccId==null || r.ncAccId.startsWith("NC_") || r.ncAccId.startsWith("NG_") ) {
            return;
        }

        r.chr = null;

        // truncate dot part of transcript acc id
        String accId = r.ncAccId;
        int dotPos = accId.indexOf(".");
        if( dotPos>0 )
            accId = accId.substring(0, dotPos);

        Connection conn = getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
                "SELECT md.start_pos, md.stop_pos,md.chromosome,md.strand FROM maps_data md,transcripts WHERE acc_id=? AND map_key=? AND transcript_rgd_id=rgd_id");
        ps.setString(1, accId);
        ps.setInt(2, 17);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            if( r.chr!=null ) {
                System.out.println("multiple positions for "+accId);
            } else {
                r.geneStartPos = rs.getInt(1);
                r.chr = rs.getString(3);
                r.strand = rs.getString(4);
            }
        }
        conn.close();
    }

    boolean getVarPos(Record r, int mapKey) throws SQLException {

        r.varChr = null;

        Connection conn = getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
                "SELECT md.start_pos, md.stop_pos,md.chromosome FROM maps_data md WHERE md.rgd_id=? AND map_key=? AND chromosome<>'Y'");
        ps.setInt(1, r.varRgdId);
        ps.setInt(2, mapKey);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            if( r.varChr!=null ) {
                System.out.println("multiple positions for varRgdId"+r.varRgdId);
                r.varChr=null;
                break;
            } else {
                r.varStartPos = rs.getInt(1);
                r.varStopPos = rs.getInt(2);
                r.varChr = rs.getString(3);
            }
        }
        conn.close();

        return r.varChr!=null;
    }

    String loadPrimaryHgvsName(Record r) throws Exception {

        r.primaryHgvsName = null;

        Connection conn = getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT hgvs_name,hgvs_name_type FROM hgvs_names WHERE rgd_id=? ORDER BY "+
            "DECODE(hgvs_name_type,'genomic_refseqgene',1,'coding_refseq',2,'genomic_toplevel',3,'coding',4,5)");
        ps.setInt(1, r.varRgdId);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            String hgvsName = rs.getString(1);
            String hgvsType = rs.getString(2);
            if( r.primaryHgvsName==null )
                r.primaryHgvsName = hgvsName;
            if( hgvsType.startsWith("protein") && !r.primaryHgvsName.contains("(p.") ) {
                // extract protein change from hgvs name
                int pos = hgvsName.indexOf("p.");
                if( pos>=0 )
                    r.primaryHgvsName += " ("+hgvsName.substring(pos)+")";
            }
        }
        conn.close();

        return r.primaryHgvsName;
    }

    DataSource getDataSource() {
        return (DataSource) (XmlBeanFactoryManager.getInstance().getBean("DataSource"));
    }

    void loadVarRgdId2RsIdMap() throws Exception {

        varRgdId2RsId.clear();
        XdbIdDAO dao = new XdbIdDAO();

        for( XdbId id: dao.getActiveXdbIds(48, RgdId.OBJECT_KEY_VARIANTS) ) {
            varRgdId2RsId.put(id.getRgdId(), id.getLinkText());
        }
    }

    class Record {
        int varRgdId; // variant rgd id
        String type; // variant type
        String name; // variant name, f.e. FAM83H:c.749C>T (p.Ser250Phe)
        String name2; // variant name without protein change, f.e. FAM83H:c.749C>T
        String locus; // relative variant locus, f.e. 749
        String nucChange; //nucleotide change, f.e. C>T
        int pos; // absolute position of variant
        String refNuc;
        String varNuc;

        String proteinChange; // fe p.Ser250Phe
        String geneSymbol; // fe FAM83H
        int geneRgdId;
        String ncAccId; // only for variants not associated with single gene, fe NC_000005.10
        String strand;

        String chr = null; // chromosome
        int geneStartPos = 0; // as read from RGD db
        String primaryHgvsName; // best-match from hgvs names

        String varChr = null; // chromosome for variant
        int varStartPos = 0; // variant start pos
        int varStopPos = 0; // variant stop pos
    }
}
