package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mtutaj on 4/13/2017.
 */
public class VcfToCommonFormat2Base {

    String getChromosome(String chr) throws Exception {

        String c = getChromosomeImpl(chr);
        if( c!=null && c.equals("M") ) {
            c = "MT";
        }
        return c;
    }

    // validate chromosome
    String getChromosomeImpl(String chr) throws Exception {
        // chromosomes could be provided as 'NC_005100.4'
        if( chr.startsWith("NC_") ) {
            return getChromosomeFromDb(chr);
        }

        chr = chr.replace("chr", "").replace("c", "");
        // skip lines with invalid chromosomes (chromosome length must be 1 or 2
        if( chr.length()>2 || chr.contains("r") || chr.equals("Un") ) {
            return null;
        }
        return chr;
    }

    String getChromosomeFromDb(String refseqNuc) throws Exception {
        String chr = _chromosomeMap.get(refseqNuc);
        if( chr==null ) {
            MapDAO dao = new MapDAO();
            Chromosome c = dao.getChromosome(refseqNuc);
            if( c!=null ) {
                chr = c.getChromosome();
                _chromosomeMap.put(refseqNuc, chr);
            }
        }
        return chr==null ? null : chr;
    }
    Map<String,String> _chromosomeMap = new HashMap<>();
}
