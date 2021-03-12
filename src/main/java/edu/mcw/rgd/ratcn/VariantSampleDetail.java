package edu.mcw.rgd.ratcn;

/**
 * @author hsnalabolu
 * @since 03/06/2020
 */
public class VariantSampleDetail {

    long id;
    String zygosityStatus;
    int zygosityPercentRead;  // Also Know as just Percent Read
    String zygosityPossibleError;
    String zygosityRefAllele;
    int zygosityNumberAllele;
    int variantFrequency;
    String zygosityInPseudo;
    int depth;
    int qualityScore;
    int sampleId;
    String source;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getZygosityStatus() {
        return zygosityStatus;
    }

    public void setZygosityStatus(String zygosityStatus) {
        this.zygosityStatus = zygosityStatus;
    }

    public int getZygosityPercentRead() {
        return zygosityPercentRead;
    }

    public void setZygosityPercentRead(int zygosityPercentRead) {
        this.zygosityPercentRead = zygosityPercentRead;
    }

    public String getZygosityPossibleError() {
        return zygosityPossibleError;
    }

    public void setZygosityPossibleError(String zygosityPossibleError) {
        this.zygosityPossibleError = zygosityPossibleError;
    }

    public String getZygosityRefAllele() {
        return zygosityRefAllele;
    }

    public void setZygosityRefAllele(String zygosityRefAllele) {
        this.zygosityRefAllele = zygosityRefAllele;
    }

    public int getZygosityNumberAllele() {
        return zygosityNumberAllele;
    }

    public void setZygosityNumberAllele(int zygosityNumberAllele) {
        this.zygosityNumberAllele = zygosityNumberAllele;
    }

    public int getVariantFrequency() {
        return variantFrequency;
    }

    public void setVariantFrequency(int variantFrequency) {
        this.variantFrequency = variantFrequency;
    }

    public String getZygosityInPseudo() {
        return zygosityInPseudo;
    }

    public void setZygosityInPseudo(String zygosityInPseudo) {
        this.zygosityInPseudo = zygosityInPseudo;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getQualityScore() {
        return qualityScore;
    }

    public void setQualityScore(int qualityScore) {
        this.qualityScore = qualityScore;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
