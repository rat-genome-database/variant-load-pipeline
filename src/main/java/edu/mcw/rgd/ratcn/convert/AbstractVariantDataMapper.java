package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.dao.DataSourceFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jdepons
 * Date: Oct 27, 2010
 * Time: 9:28:05 AM
 * <p>
 * code copied from carpenovo loading project
 */
public class AbstractVariantDataMapper {

    protected Map filePointers = new HashMap();

    private int count = 0;
    private String baseOutputDir;
    private int mapKey = 70;
    private String dbSnpSource = "dbSnp138";

    private Connection rgdConn;

    public AbstractVariantDataMapper() {
    }

    // by default both 'Variants' and 'Variant Reads' directories are located under directory
    // HOMEDIR/output/JOBNAME ( ==base output directory)
    //     HOMEDIR/output/JOBNAME/Variations
    //     HOMEDIR/output/JOBNAME/Assembly/Parsed_XX-XX-XX
    //
    public String getBaseOutputDirectory() {
        return baseOutputDir;
    }

    // override base output directory; subdirs will be created under this new dir
    //     NEW_BASE_OUTPUT_DIR/Variations
    //     NEW_BASE_OUTPUT_DIR/Assembly/Parsed_XX-XX-XX
    public void setBaseOutputDirectory(String newBaseOutputDir) throws IOException {
        this.baseOutputDir = newBaseOutputDir;
    }

    /**
     * Returns the top level direcotry to the Variants directory
     *
     * @return
     */
    public String getOutputDirectory(String strain) {
        return getBaseOutputDirectory() + "/" + strain + "/Variations";
    }

    /**
     * Returns the top level path the the Variant Read Directory.
     *
     * @return
     */
    public String getVariantReadDirectory(String strain) {
        return getBaseOutputDirectory() + "/" + strain + "/Assembly/Parsed_XX-XX-XX";
    }


    private void writeToError(String strain, VariantRecord vr, Exception e) throws Exception {
        System.out.println("WARNING: " + e.getMessage());

        BufferedWriter errorFile = getFileWriter(strain, null, null);
        errorFile.write(vr.getChromosome() + "\t");
        errorFile.write(vr.getStart() + "\t");
        errorFile.write(vr.getStop() + "\t");
        errorFile.write(vr.getReferenceNucleotide() + "\t");
        errorFile.write(vr.getVariantNucleotide() + "\t");
        errorFile.write(vr.getQualityScore() + "\t");
        errorFile.write(vr.getZygosity() + "\t");
        errorFile.write(vr.getRsNumber() + "\t");
        errorFile.write(vr.getReadDepth() + "\t");
        errorFile.write(e.getMessage() + "\n");
        errorFile.write(vr.getZygosityPercentRead() + "\t");
    }

    /**
     * Write Variant Read Record  to the file system in the correct format for pick up and processing by
     * the  LoadZygosityDepth job
     *
     * @param vr VariantReads
     */
    public void writeVarRead(String strain, VariantReads vr) throws Exception {

        BufferedWriter out = this.getVariantReadWriter(strain, vr.getChromosome());
        // Format : "#position\tA\tC\tG\tT\tmodified_call\ttotal\tused\tscore\treference\ttype\n");

        out.write(vr.getPosition() + "\t"
                + vr.getReadCountA() + "\t"
                + vr.getReadCountC() + "\t"
                + vr.getReadCountG() + "\t"
                + vr.getReadCountT() + "\t"
                + vr.getModifiedCall() + "\t"
                + "\t"
                + "\t"
                + "\t"
                + vr.getRefNuc() + "\t"
                + "\t"
                + "\n");
        //out.flush();

    }

    /**
     * Writes a VariantRecord to the file system for later pickup by variantIlluminaLoad program
     *
     * @param vr
     * @throws Exception
     */
    public void write(String strain, VariantRecord vr) throws Exception {

        if (IUPAC.getNucleotide(vr.getVariantNucleotide()).equalsIgnoreCase("Unknown")) {
            this.writeToError(strain, vr, new Exception("(Variant Nucleotide Not Available)"));
        }

        // remove below? 'N' has mapped to 'ATCG'.
        if (vr.getReferenceNucleotide().equalsIgnoreCase("N")) {
            this.writeToError(strain, vr, new Exception("Reference Nucleotide = N"));
            return;
        }

        this.validate(vr);

        if (vr.getRsNumber() == null) {

            boolean match = false;
            String alleles = null; // all alleles in DB_SNP on this position, comma separated

            for( String[] alleleInfo: getAllelesFromDbSnp(vr.getStart(), vr.getChromosome()) ) {
                String allele = alleleInfo[0];
                String snpName = alleleInfo[1];

                if( allele.substring(0,1).equals(vr.getVariantNucleotide()) ) {
                    vr.setRsNumber(snpName);
                    match = true;
                    break;
                }
                //if either allele match ref and either of the allels matches the variant then map it

                if( alleles==null ) {
                    alleles = allele; }
                else {
                    alleles += "," + allele; }
            }

            if( !match && alleles!=null ) {
                System.out.println("No Match '" + alleles + "' on position " + vr.getStart());
            }
        }
        BufferedWriter out = this.getFileWriter(strain, vr.getChromosome(), vr.getRsNumber());



        String hgvsName="", rgdId="";
        if( vr.getVarName()!=null ) {
            String[] varNameParts = vr.getVarName().split("[;]");
            if( varNameParts.length>0 ) {
                rgdId = varNameParts[0];
                if( rgdId.startsWith("RGDID:") )
                    rgdId = rgdId.substring(6);
                else
                    rgdId = "";
            }
            if( varNameParts.length>1 ) {
                hgvsName = varNameParts[1];
            }
        }

        // Here is where we actually write the line to the file in dbSNP or NovelSNP format based on if an RSNumber was
        // Specified.
        if (vr.getRsNumber() == null) {
            // novelDBSnp Format
            out.write(vr.getStart() + "\t" +
                    vr.getVariantNucleotide() + "\t" +
                    vr.getQualityScore() + "\t" +
                    vr.getReferenceNucleotide() + "\t" +
                    vr.getZygosityPercentRead() + "\t" +
                    ((vr.getZygosity() == null) ? "" : vr.getZygosity()) + "\t" +
                    ((vr.getReadDepth() == -1) ? "" : vr.getReadDepth()) + "\t" +
                    hgvsName + "\t" +
                    rgdId + "\n");
        } else {
            // dbSnp Format
            out.write(vr.getStart() + "\t" +
                    vr.getRsNumber() + "\t" +
                    "*" + "\t" +
                    "*" + "\t" +
                    vr.getReferenceNucleotide() + "\t" +
                    vr.getVariantNucleotide() + "\t" +
                    vr.getQualityScore() + "\t" +
                    vr.getZygosityPercentRead() + "\t" +
                    ((vr.getZygosity() == null) ? "" : vr.getZygosity()) + "\t" +
                    ((vr.getReadDepth() == -1) ? "" : vr.getReadDepth()) + "\t" +
                    hgvsName + "\t" +
                    rgdId + "\n");
        }

        //out.flush();

        this.count++;
        if (this.count % 1000 == 0) {
            System.out.println("Processed " + count + " records");
        }


    }

    /**
     * Returns a BufferWriter to the correct location of the Variant Read file given the
     * chromosome provided. Creates the directory path as needed to this file.
     *
     * @param chromosome
     * @return
     * @throws Exception
     */
    private BufferedWriter getVariantReadWriter(String strain, String chromosome) throws Exception {

        String outputDir = this.getVariantReadDirectory(strain);
        outputDir += "/c" + chromosome.toUpperCase() + ".fa";
        File dir = new File(outputDir);

        String outputFile = outputDir + "/c" + chromosome.toUpperCase() + ".fa.snp.txt";
        BufferedWriter out = (BufferedWriter) this.filePointers.get(outputFile);
        if (out == null) {

            // when adding a file to cache, ensure the path is valid
            if (!dir.exists()) {
                dir.mkdirs();
            }

            out = new BufferedWriter(new FileWriter(outputFile));
            // Start out with a header from casava 1.7
            out.write("#position\tA\tC\tG\tT\tmodified_call\ttotal\tused\tscore\treference\ttype\n");
            this.filePointers.put(outputFile, out);
        }

        return out;

    }

    private BufferedWriter getFileWriter(String strain, String chromosome, String rsNumber) throws Exception {
        String outputFile = this.getOutputDirectory(strain);
        if( chromosome==null ) {
            outputFile += "/log/error.txt";
        }
        else if (rsNumber == null) {
            outputFile += "/novelSNP/c" + chromosome.toUpperCase() + ".fa_novel_SNP.txt";
        } else {
            outputFile += "/dbSNP/c" + chromosome + ".fa_custom_dbSNP.txt";
        }

        BufferedWriter out = (BufferedWriter) this.filePointers.get(outputFile);
        if (out == null) {
            // ensure the directory is available
            int pos = outputFile.lastIndexOf('/');
            File file = new File(outputFile.substring(0, pos));
            file.mkdirs();

            out = new BufferedWriter(new FileWriter(outputFile));

            if (rsNumber == null) {
                out.write("position\tobserved_genotype\tobserved_allele_score\tref_seq\tzygosity\tread_depth\thgvs_name\trgd_id\n");
            } else if( chromosome!=null ) {
                out.write("position\trsID\tdbSNP_alleles\tdbSNP_ref_orientation\tref_seq\tobserved_genotype\tobserved_allele_score\tzygosity\tread_depth\thgvs_name\trgd_id\n");
            }

            this.filePointers.put(outputFile, out);
        }

        return out;
    }


    public void validate(VariantRecord vr) throws Exception {

        // hack: ref or var nucleotide could contain '-' (this does not happen in variants strictly following vcf naming
        if( !vr.getReferenceNucleotide().equals("-") )
            vr.setReferenceNucleotide(IUPAC.getNucleotide(vr.getReferenceNucleotide()));
        if( !vr.getVariantNucleotide().equals("-") )
            vr.setVariantNucleotide(IUPAC.getNucleotide(vr.getVariantNucleotide()));
    }

    public void open() throws Exception {

        rgdConn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
    }

    public void close() throws Exception {

        for (Object o : this.filePointers.keySet()) {
            BufferedWriter bw = (BufferedWriter) this.filePointers.get(o);
            try {
                bw.close();
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }

        rgdConn.close();
        rgdConn = null;
    }


    // we cache last position and chromosome
    long lastPos;
    String lastChr = "";
    List<String[]> lastAlleles = null;

    List<String[]> getAllelesFromDbSnp(long pos, String chr) throws Exception {

        // check if data is available from cache
        if( pos==lastPos && lastChr.equals(chr) ) {
            return lastAlleles;
        }
        // data is not in the cache
        lastChr = chr;
        lastPos = pos;
        lastAlleles = new ArrayList<String[]>();

        String sql = "select allele,snp_name from db_snp where position=? and map_key=? and source=? and chromosome=? and snp_class='snp'";

        PreparedStatement stmt = rgdConn.prepareStatement(sql);

        stmt.setLong(1, pos);
        stmt.setInt(2, this.getMapKey());
        stmt.setString(3, this.getDbSnpSource());
        stmt.setString(4, chr);

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            String[] alleleInfo = new String[2];
            alleleInfo[0] = rs.getString("allele").toUpperCase();
            alleleInfo[1] = rs.getString("snp_name");
            lastAlleles.add(alleleInfo);
        }

        stmt.close();

        return lastAlleles;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public String getDbSnpSource() {
        return dbSnpSource;
    }

    public void setDbSnpSource(String dbSnpSource) {
        this.dbSnpSource = dbSnpSource;
    }
}
