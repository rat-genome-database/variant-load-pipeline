package edu.mcw.rgd.ratcn.convert;

/**
 * @author mtutaj
 * @since 10/30/14
 * represents a line to be written into a file in common-format-2
 * <p>
 * columns of common format 2:
 * 1. chr
 * 2. position
 * 3. ref nucleotide
 * 4. var nucleotide (allele) -- 1 allele per line
 * 5. rsId -- if available
 * 6. A count - count of occurrences of A genotype (AD field in VCF files)
 * 7. C count (AD field in VCF files)
 * 8. G count (AD field in VCF files)
 * 9. T count (AD field in VCF files)
 * 10. total_depth (DP field in VCF files)
 * 11. hgvs_name
 * 12. rgd_id
 * 13. allele_depth (AD field: how many reads were called for this allele)
 * 14. allele_count (count of all alleles at this position)
 * 15. read_depth (sum of allele depths for all alleles; sum of values of AD field)
 * 16. padding_base (if available in incoming data, usually for indels from data submitted in VCF format)
 *
 * Note: especially for indels it is common to see lines like that:
 * ref:G, alleles:GT,GTT     GT:AD:DP:GQ:PL      0/2:3,2,7:16:52:260,139,174,0,52,74
 *   total_depth=16
 *   read_depth=3+2+7=12
 *   GT_allele_depth=2
 *   GTT_allele_depth=7
 *   allele_count=3
 */
public class CommonFormat2Line {

    private String chr;
    private int pos;
    private String refNuc;
    private String varNuc;
    private String rsId;
    private Integer countA;
    private Integer countC;
    private Integer countG;
    private Integer countT;
    private Integer totalDepth;
    private String hgvsName;
    private Integer rgdId;
    private Integer alleleDepth;
    private Integer alleleCount;
    private Integer readDepth;
    private String paddingBase;

    // return true if adjustement was successful
    public boolean adjustForIndels() {
        // snv
        if( refNuc.length()==1 && varNuc.length()==1 ) {
            return true;
        }
        // insertion
        if( refNuc.length()==1 && varNuc.length()>1 ) {
            // varNuc first base must be the same as refNuc
            if( refNuc.charAt(0)!=varNuc.charAt(0) ) {
                System.out.println("Unexpected: insertion: missing padding base");
                return false;
            }
            // handle padding base
            setPaddingBase(refNuc);
            setRefNuc(null);
            setVarNuc(varNuc.substring(1));
            pos++;
            return true;
        }
        // deletion
        if( refNuc.length()>1 && varNuc.length()==1 ) {
            // varNuc first base must be the same as refNuc
            if( refNuc.charAt(0)!=varNuc.charAt(0) ) {
                System.out.println("Unexpected: deletion: missing padding base");
                return false;
            }
            // handle padding base
            setPaddingBase(varNuc);
            setVarNuc(null);
            setRefNuc(refNuc.substring(1));
            pos++;
            return true;
        }
        // unhandled case
        System.out.println("TODO");
        return false;
    }

    public boolean old_adjustForIndels() {
        while( refNuc.length()>1 && varNuc.length()>0 ) {
            if( varNuc.length()>refNuc.length() ) {
                // strip same nucleotides from the end
                //if( refNuc.charAt(refNuc.length()-1)==varNuc.charAt(varNuc.length()-1) ) {
                //    refNuc = refNuc.substring(0, refNuc.length()-1);
                //    varNuc = varNuc.substring(0, varNuc.length()-1);
                //}
                //else
                if( refNuc.charAt(0)==varNuc.charAt(0) ) {
                    // strip first nucleotide from refNuc and varNuc
                    refNuc = refNuc.substring(1);
                    varNuc = varNuc.substring(1);
                    pos++;
                } else {
                    System.out.println("unhandled insertion");
                    return false;
                }
            } else {
                // deletion
                while( !varNuc.isEmpty() ) {
                    if( refNuc.charAt(0)==varNuc.charAt(0) ) {
                        // strip first nucleotide from refNuc and varNuc
                        refNuc = refNuc.substring(1);
                        varNuc = varNuc.substring(1);
                        pos++;
                        //} else if( refNuc.charAt(refNuc.length()-1)==varNuc.charAt(varNuc.length()-1) ) {
                        //    // strip last nucleotide from refNuc and varNuc
                        //    refNuc = refNuc.substring(0, refNuc.length()-1);
                        //    varNuc = varNuc.substring(0, varNuc.length()-1);
                    } else {
                        // irregular deletion, f.e. AAG -> T
                        while( refNuc.length()>varNuc.length() ) {
                            varNuc += "-";
                        }
                        return true;
                    }
                }
            }
        }

        // deletion final
        while( refNuc.length()>varNuc.length() ) {
            varNuc += "-";
        }
        return true;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String getRefNuc() {
        return refNuc;
    }

    public void setRefNuc(String refNuc) {
        this.refNuc = refNuc;
    }

    public String getVarNuc() {
        return varNuc;
    }

    public void setVarNuc(String varNuc) {
        this.varNuc = varNuc;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public Integer getCountA() {
        return countA;
    }

    public void setCountA(Integer countA) {
        this.countA = countA;
    }

    public Integer getCountC() {
        return countC;
    }

    public void setCountC(Integer countC) {
        this.countC = countC;
    }

    public Integer getCountG() {
        return countG;
    }

    public void setCountG(Integer countG) {
        this.countG = countG;
    }

    public Integer getCountT() {
        return countT;
    }

    public void setCountT(Integer countT) {
        this.countT = countT;
    }

    public Integer getTotalDepth() {
        return totalDepth;
    }

    public void setTotalDepth(Integer totalDepth) {
        this.totalDepth = totalDepth;
    }

    public String getHgvsName() {
        return hgvsName;
    }

    public void setHgvsName(String hgvsName) {
        this.hgvsName = hgvsName;
    }

    public Integer getRgdId() {
        return rgdId;
    }

    public void setRgdId(Integer rgdId) {
        this.rgdId = rgdId;
    }

    public Integer getAlleleDepth() {
        return alleleDepth;
    }

    public void setAlleleDepth(Integer alleleDepth) {
        this.alleleDepth = alleleDepth;
    }

    public Integer getAlleleCount() {
        return alleleCount;
    }

    public void setAlleleCount(Integer alleleCount) {
        this.alleleCount = alleleCount;
    }

    public Integer getReadDepth() {
        return readDepth;
    }

    public void setReadDepth(Integer readDepth) {
        this.readDepth = readDepth;
    }

    public String getPaddingBase() {
        return paddingBase;
    }

    public void setPaddingBase(String paddingBase) {
        this.paddingBase = paddingBase;
    }
}
