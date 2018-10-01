package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.ratcn.ChrFastaFile;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 2/3/14
 * Time: 3:46 PM
 * <p>
 * mflister is providing Excel files with variants, one file per chromosome
 * every Excel sheet contains: position, and observed nucleotide for 6 strains
 */
public class Txt2Vcf {

    static boolean headerWritten = false;

    public static void main(String[] args) throws Exception {

        String fname2 = "/tmp/mflister.vcf";
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname2));

        File dir = new File("s:\\mflister");
        for( File f: dir.listFiles() ) {
            if( f.isFile() && f.getName().endsWith(".txt") ) {
                processFile(f, writer);
            }
        }

        writer.close();

        System.out.println("---OK---");
    }

    static void processFile(File file, BufferedWriter writer) throws Exception {

        String filePath = file.getAbsolutePath();
        System.out.println("processing "+filePath);

        // determine chromosome from file name
        int pos = filePath.lastIndexOf("chr");
        if( pos < 0 ) {
            System.out.println("Unexpected file name: "+filePath);
            return;
        }
        String chr = filePath.substring(pos+3, filePath.length()-4);

        ChrFastaFile fastaFile = new ChrFastaFile();
        fastaFile.initForChromosome("/data/ref/fasta/rn4", chr);

        String line;
        // 2 first lines of src file
        //chr	pos	F.SS1	M.SS1	F.FHH	M.FHH	F.PCK	M.PCK
        // 13	874	G	G	T	T	G	G
        // 13	"1,079"	A	A	T	T	A	A

        // 2 first lines of dest file

        BufferedReader reader = new BufferedReader(new FileReader(file));

        line = reader.readLine().trim();
        String[] header = line.split("[\\t]", -1);
        if( !headerWritten ) {
            writer.write("##fileformat=VCFv4.1\n");
            writer.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT");
            for( int col=2; col<header.length; col++ ) {
                writer.write("\t"+header[col]);
            }
            writer.write('\n');

            headerWritten=true;
        }

        int linesProcessed = 0;
        int linesWithIssues = 0;
        int linesSameAsRef = 0;
        int refNucIsN = 0;

        while( (line=reader.readLine())!=null ) {
            line = line.trim();
            linesProcessed++;
            String[] cols = line.split("[\\t]", -1);
            String posAsString = readPos(cols[1]);
            pos = Integer.parseInt(posAsString);

            String refNuc = fastaFile.getDnaChunk(pos, pos+1).toUpperCase();
            if( refNuc.equals("N") ) {
                refNucIsN++;
                continue;
            }

            String[] alleles = readAlleles(Arrays.copyOfRange(cols, 2, cols.length), refNuc);
            int alleleCount = alleles.length;
            if( alleleCount<=1 ) {
                linesSameAsRef++;
                continue;
            }

            // CHROM
            writer.write(cols[0]);

            // POS
            writer.write("\t" + posAsString);

            // ID
            writer.write("\t.");

            // REF
            writer.write("\t" + refNuc);

            // ALT
            writer.write("\t");
            // write out all alleles, ',' separated
            for( int i=1; i<alleleCount; i++ ) {
                if( i>1 )
                    writer.write(",");
                writer.write(alleles[i]);
            }

            // QUAL
            writer.write("\tPASS");

            // FILTER
            writer.write("\tVALIDATED=1");

            // INFO
            writer.write("\t");

            // FORMAT
            writer.write("\tGT;AD");

            int i;
            for( i=2; i<2+6; i++ ) {
                if( !writeData(cols[i], alleles, writer) )
                    break;
            }

            if( i<2+6 )
                linesWithIssues++;

            writer.write("\n");
        }


        reader.close();

        fastaFile.release();

        System.out.println("linesProcessed    ="+linesProcessed);
        System.out.println("  linesWithIssues ="+linesWithIssues);
        System.out.println("  linesSameAsRef  ="+linesSameAsRef);
        if( refNucIsN>0 )
            System.out.println("  linesRefNucIsN  ="+refNucIsN);
    }

    static boolean writeData(String varNuc, String[] alleles, BufferedWriter writer) throws Exception {

        // handle no-data case
        if( varNuc.equals("N") ) {
            writer.write("\t./.:0,0");
            return true;
        }

        // handle homozygotes
        if( varNuc.equals("H") ) {

            // homozygote -- use first available allele
            varNuc = alleles[1];
            String alleleDepths = ":0";
            for( int i=1; i<alleles.length; i++ ) {
                if( varNuc.equals(alleles[i]) ) {
                    alleleDepths += ",9";
                } else {
                    alleleDepths += ",0";
                }
            }

            for( int i=1; i<alleles.length; i++ ) {
                if( varNuc.equals(alleles[i]) ) {
                    writer.write("\t1/"+i+alleleDepths);
                    return true;
                }
            }
            System.out.println("consistency problem for H");
            return false;
        }

        String alleleDepths = ":9";
        for( int i=1; i<alleles.length; i++ ) {
            if( varNuc.equals(alleles[i]) ) {
                alleleDepths += ",9";
            } else {
                alleleDepths += ",0";
            }
        }

        for( int i=0; i<alleles.length; i++ ) {
            if( varNuc.equals(alleles[i]) ) {
                writer.write("\t0/"+i+alleleDepths);
                return true;
            }
        }
        System.out.println("consistency problem");
        return false;
    }

    static String[] readAlleles(String[] data, String refNuc) {
        List<String> alleles = new ArrayList<String>();
        alleles.add(refNuc);
        for( String allele: data ) {
            if( allele.equals("N") )
                continue;
            if( allele.equals("H") )
                continue;
            if( !alleles.contains(allele) )
                alleles.add(allele);
        }
        return alleles.toArray(new String[]{});
    }

    // position could be given as ["1,079"] ; convert it to [1079]
    public static String readPos(String pos) {

        StringBuilder buf = new StringBuilder();
        for( int i=0; i<pos.length(); i++ ) {
            char c = pos.charAt(i);
            if( Character.isDigit(c) )
                buf.append(c);
        }
        return buf.toString();
    }
}
