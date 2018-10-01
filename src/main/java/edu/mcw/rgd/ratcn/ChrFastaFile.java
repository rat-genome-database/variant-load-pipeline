package edu.mcw.rgd.ratcn;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 8/9/13
 * Time: 1:05 PM
 */
public class ChrFastaFile {

    private RandomAccessFile memoryMappedFile;
    private MappedByteBuffer buf;
    private String currentFastaFile = null;

    public boolean initForChromosome(String fastaDir, String chr) throws Exception {

        String fastaFile = fastaDir+"/chr"+chr+".nuc";

        // reuse the old file if same chromosome
        if( currentFastaFile!=null && currentFastaFile.equals(fastaFile) ) {
            return true;
        }
        // different chromosome
        if( currentFastaFile!=null ) {
            release();
        }

        File file = new File(fastaFile);
        if( !file.exists() ) {
            // nuc file does not exist -- create a nuc file
            if( !createNucFile(fastaDir, chr) )
                return false;
            if( !file.exists() )
                return false;
        }

        memoryMappedFile = new RandomAccessFile(fastaFile, "r");
        buf = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        currentFastaFile = fastaFile;
        return true;
    }

    boolean createNucFile(String fastaDir, String chr) throws Exception {

        String[] chrPrefixes = {"chr","Chr","ch","Ch","c","C",""};
        String fastaFile = null;
        for( String chrPrefix: chrPrefixes ) {
            fastaFile = fastaDir+"/"+chrPrefix+chr+".fa.gz";
            if( new File(fastaFile).exists() ) {
                break;
            } else {
                fastaFile = fastaDir+"/"+chrPrefix+chr+".fna.gz";
                if( new File(fastaFile).exists() ) {
                    break;
                } else {
                    fastaFile = null;
                }
            }
        }
        if( fastaFile==null ) {
            System.out.println("ERROR: Cannot find fasta file for chromosome "+chr+" in "+fastaDir);
            return false;
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fastaFile))));
        BufferedWriter wr = new BufferedWriter(new FileWriter(fastaDir+"/chr"+chr+".nuc"));
        String line;
        int totalLen = 0;
        while ((line = br.readLine())!=null) {
            if (!(line.equals("") || line.startsWith(">"))) {
                String str = line.trim();
                totalLen += str.length();
                wr.write(str);
            }
        }
        br.close();
        wr.close();
        System.out.println("Found dna of size : " + totalLen + " for chr " + chr);

        return true;
    }

    public void release() throws Exception {
        buf = null;
        if( memoryMappedFile!=null )
            memoryMappedFile.close();
        memoryMappedFile = null;
        currentFastaFile = null;
    }

    public String getDnaChunk(int startPos, int stopPos) {
        byte[] b = new byte[stopPos-startPos];
        for( int i=startPos; i<stopPos; i++ ) {
            b[i-startPos] = buf.get(i);
        }
        return new String(b);
    }
}
