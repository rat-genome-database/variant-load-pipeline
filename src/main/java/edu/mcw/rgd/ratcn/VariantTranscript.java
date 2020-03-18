package edu.mcw.rgd.ratcn;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 11/15/13
 * Time: 12:28 PM
 * <p>
 * models a rows from VARIANT_TRANSCRIPT table
 */
public class VariantTranscript {
    private long id;
    private long variantId;
    private int transcriptRgdId;
    private String refAA;
    private String varAA;
    private String genespliceStatus;
    private String polyphenStatus;
    private String synStatus;
    private String locationName;
    private String nearSpliceSite;
    private int fullRefNucSeqKey;
    private Integer fullRefNucPos;
    private int fullRefAASeqKey;
    private Integer fullRefAAPos;
    private String tripletError;
    private String frameShift;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getVariantId() {
        return variantId;
    }

    public void setVariantId(long variantId) {
        this.variantId = variantId;
    }

    public int getTranscriptRgdId() {
        return transcriptRgdId;
    }

    public void setTranscriptRgdId(int transcriptRgdId) {
        this.transcriptRgdId = transcriptRgdId;
    }

    public String getRefAA() {
        return refAA;
    }

    public void setRefAA(String refAA) {
        this.refAA = refAA;
    }

    public String getVarAA() {
        return varAA;
    }

    public void setVarAA(String varAA) {
        this.varAA = varAA;
    }

    public String getGenespliceStatus() {
        return genespliceStatus;
    }

    public void setGenespliceStatus(String genespliceStatus) {
        this.genespliceStatus = genespliceStatus;
    }

    public String getPolyphenStatus() {
        return polyphenStatus;
    }

    public void setPolyphenStatus(String polyphenStatus) {
        this.polyphenStatus = polyphenStatus;
    }

    public String getSynStatus() {
        return synStatus;
    }

    public void setSynStatus(String synStatus) {
        this.synStatus = synStatus;
    }

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public String getNearSpliceSite() {
        return nearSpliceSite;
    }

    public void setNearSpliceSite(String nearSpliceSite) {
        this.nearSpliceSite = nearSpliceSite;
    }

    public Integer getFullRefNucPos() {
        return fullRefNucPos;
    }

    public void setFullRefNucPos(Integer fullRefNucPos) {
        this.fullRefNucPos = fullRefNucPos;
    }

    public Integer getFullRefAAPos() {
        return fullRefAAPos;
    }

    public void setFullRefAAPos(Integer fullRefAAPos) {
        this.fullRefAAPos = fullRefAAPos;
    }

    public String getTripletError() {
        return tripletError;
    }

    public void setTripletError(String tripletError) {
        this.tripletError = tripletError;
    }

    public String getFrameShift() {
        return frameShift;
    }

    public void setFrameShift(String frameShift) {
        this.frameShift = frameShift;
    }

    public int getFullRefNucSeqKey() {
        return fullRefNucSeqKey;
    }

    public void setFullRefNucSeqKey(int fullRefNucSeqKey) {
        this.fullRefNucSeqKey = fullRefNucSeqKey;
    }

    public int getFullRefAASeqKey() {
        return fullRefAASeqKey;
    }

    public void setFullRefAASeqKey(int fullRefAASeqKey) {
        this.fullRefAASeqKey = fullRefAASeqKey;
    }
}