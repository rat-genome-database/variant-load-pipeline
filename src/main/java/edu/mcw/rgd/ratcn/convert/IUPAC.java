package edu.mcw.rgd.ratcn.convert;

import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: jdepons
 * Date: Nov 3, 2010
 * Time: 5:09:59 PM
 */
public class IUPAC {

    private static HashMap IUPACMap = null;


    public static String getNucleotide(String value) throws Exception{
        if (value.length() > 1) return value;

        if (IUPACMap == null) {

            IUPACMap = new HashMap<String, String>();
            IUPACMap.put("A", "A");
            IUPACMap.put("C", "C");
            IUPACMap.put("G", "G");
            IUPACMap.put("T", "T");
            IUPACMap.put("R", "AG");
            IUPACMap.put("Y", "CT");
            IUPACMap.put("M", "AC");
            IUPACMap.put("K", "GT");
            IUPACMap.put("S", "GC");
            IUPACMap.put("W", "AT");
            IUPACMap.put("H", "ACT");
            IUPACMap.put("B", "CGT");
            IUPACMap.put("V", "ACG");
            IUPACMap.put("D", "AGT");
            IUPACMap.put("N", "ATCG");
            //IUPACMap.put("-", "-");  //for indel
        }

        String return_str = (String) IUPACMap.get(value);
        if (return_str == null) return_str = "Unknown";
        
        return return_str;
    }

}
