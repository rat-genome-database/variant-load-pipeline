package edu.mcw.rgd.ratcn.convert;

/**
 * @author jdepons
 * @since Oct 27, 2010
 */
public class VariantRecord {

    private String chromosome;
    private String zygosity;
    private int zygosityPercentRead = -1;
    private int readDepth = -1;
    private Long start = (long) -1;
    private Long stop = (long) -1;
    private String referenceNucleotide;
    private String variantNucleotide;
    private String qualityScore;
    private String rsNumber;
    private Boolean pseudoZone = null;     // indicated if variant is within psuedoautosomoal zone
    private String recordType = null;      // variant type (SNV,INS,DEL,NO CALL);
    private Boolean anyRefAllele = null;   // if variant contains any reference alleles
    private int numAlleles = -1;         // number of alleles seen at the same location as the variant
    private String varName;

    public int getNumAlleles() {
        return numAlleles;
    }

    public void setNumAlleles(int numAlleles) {
        this.numAlleles = numAlleles;
    }

    public Boolean getAnyRefAllele() {
        return anyRefAllele;
    }

    public void setAnyRefAllele(Boolean anyRefAllele) {
        this.anyRefAllele = anyRefAllele;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public Boolean getPseudoZone() {
        return pseudoZone;
    }

    public void setPseudoZone(Boolean pseudoZone) {
        this.pseudoZone = pseudoZone;
    }

    public String getRsNumber() {
        return rsNumber;
    }

    public void setRsNumber(String rsNumber) {
        this.rsNumber = rsNumber;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getZygosity() {
        return zygosity;
    }

    public void setZygosity(String zygosity) {
        this.zygosity = zygosity;
    }

    public int getReadDepth() {
        return readDepth;
    }

    public void setReadDepth(int readDepth) {
        this.readDepth = readDepth;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getStop() {
        return stop;
    }

    public void setStop(Long stop) {
        this.stop = stop;
    }

    public String getReferenceNucleotide() {
        return referenceNucleotide;
    }

    public void setReferenceNucleotide(String referenceNucleotide) {
        this.referenceNucleotide = referenceNucleotide;
    }

    public String getVariantNucleotide() {
        return variantNucleotide;
    }

    public void setVariantNucleotide(String variantNucleotide) {
        this.variantNucleotide = variantNucleotide;
    }

    public String getQualityScore() {
        return qualityScore;
    }

    public void setQualityScore(String qualityScore) {
        this.qualityScore = qualityScore;
    }

    public int getZygosityPercentRead() {
        return zygosityPercentRead;
    }

    /**
     * This is used only if zygosity Percent Read has not been set
     *
     * @param zygosityPercentRead
     */
    public void setZygosityPercentRead(int zygosityPercentRead) {
        this.zygosityPercentRead = zygosityPercentRead;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }
}
