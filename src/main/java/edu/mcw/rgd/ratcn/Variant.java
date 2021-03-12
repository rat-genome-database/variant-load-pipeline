package edu.mcw.rgd.ratcn;

/**
 * @author hsnalabolu
 * @since 03/06/2020
 */
public class Variant {

    int id;
    String referenceNucleotide = "";
    String variantNucleotide = "";
    String rsId;
    String clinvarId;
    String variantType;
    int speciesTypeKey;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public String getClinvarId() {
        return clinvarId;
    }

    public void setClinvarId(String clinvarId) {
        this.clinvarId = clinvarId;
    }

    public String getVariantType() {
        return variantType;
    }

    public void setVariantType(String variantType) {
        this.variantType = variantType;
    }

    public int getSpeciesTypeKey() {
        return speciesTypeKey;
    }

    public void setSpeciesTypeKey(int speciesTypeKey) {
        this.speciesTypeKey = speciesTypeKey;
    }
}
