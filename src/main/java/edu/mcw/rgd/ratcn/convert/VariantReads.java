package edu.mcw.rgd.ratcn.convert;

/**
 * Created by IntelliJ IDEA.
 * User: GKowalski
 * Date: 9/14/11
 * Time: 3:31 PM
 * Used to hold a Variant Read Record that is eventually written to a text file , which is later
 * read by the Load Zygosity Depth program and use to populate Depth and Zygosity
 */
public class VariantReads {

    private String position;
    private String chromosome;
    private String readCountA;
    private String readCountC;
    private String readCountG;
    private String readCountT;
    private String refNuc;
    private String modifiedCall;  // aka Variant

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getReadCountA() {
        return readCountA;
    }

    public void setReadCountA(String readCountA) {
        this.readCountA = readCountA;
    }

    public String getReadCountC() {
        return readCountC;
    }

    public void setReadCountC(String readCountC) {
        this.readCountC = readCountC;
    }

    public String getReadCountG() {
        return readCountG;
    }

    public void setReadCountG(String readCountG) {
        this.readCountG = readCountG;
    }

    public String getReadCountT() {
        return readCountT;
    }

    public void setReadCountT(String readCountT) {
        this.readCountT = readCountT;
    }

    public String getRefNuc() {
        return refNuc;
    }

    public void setRefNuc(String refNuc) {
        this.refNuc = refNuc;
    }

    public String getModifiedCall() {
        return modifiedCall;
    }

    public void setModifiedCall(String modifiedCall) {
        this.modifiedCall = modifiedCall;
    }
}
