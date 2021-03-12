package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.spring.StringListQuery;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.datamodel.Transcript;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.SqlParameter;

import java.io.*;
import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 1/9/14
 * load the polyphen log file, find all proteins that have not been processed due to not being in the internal
 * protein database and generate fasta file for all that proteins
 */
public class PolyphenFasta extends VariantProcessingBase {

    private String version;
    private String resultsDir;
    private String outputDir;

    private TranscriptDAO transcriptDAO = new TranscriptDAO();
    private SequenceDAO sequenceDAO = new SequenceDAO();

    public PolyphenFasta() throws Exception {

    }

    public static void main(String[] args) throws Exception {

        XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        PolyphenFasta instance = (PolyphenFasta) (bf.getBean("polyphenFasta"));

        // process args
        int mapKey = 0;
        String chr = null;

        for( int i=0; i<args.length; i++ ) {
            if( args[i].equals("--mapKey") ) {
                mapKey = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("--chr") ) {
                chr = args[++i];
            }
            else if( args[i].equals("--resultsDir") ) {
                instance.setResultsDir(args[++i]);
            }
            else if( args[i].equals("--outputDir") ) {
                instance.setOutputDir(args[++i]);
            }
        }

        String fileNameBase = instance.getResultsDir() + "/" + mapKey +"." + chr;
        instance.setLogWriter(new BufferedWriter(new FileWriter(fileNameBase+".fasta.log")));

        if( chr==null )
            instance.run(mapKey);
        else
            instance.run(mapKey, chr);


        instance.getLogWriter().close();
    }
    public void run(int mapKey) throws Exception {

        List<String> chromosomes = getChromosomes(mapKey);

        for( String chr: chromosomes ) {
            run(mapKey, chr);
        }
    }

    public void run(int mapKey, String chr) throws Exception {

        // read all log lines from polyphen
        // extract protein acc ids for lines starting with "ERROR: Unable to locate protein entry ";
        Set<String> setOfProteinAccIds = readProteinAccIds(mapKey, chr);
        List<String> proteinAccIds = new ArrayList<String>(setOfProteinAccIds);
        Collections.shuffle(proteinAccIds);

        // create the fasta file
        String fastaFileName = this.getOutputDir()+"/Assemby"+mapKey+"."+chr+".PolyPhenInput.fasta";
        BufferedWriter writer = new BufferedWriter(new FileWriter(fastaFileName));

        // generate fasta sequence for extracted protein acc ids
        int nr = 0;
        for( String proteinAccId: proteinAccIds ) {

            nr++;

            String sequence = getProteinSequence(proteinAccId);
            if( sequence==null ) {
                getLogWriter().write(nr+". ERROR: failed to find protein sequence for "+proteinAccId+"\n");
                continue;
            }

            getLogWriter().write(nr+". writing out protein sequence for "+proteinAccId+"\n"+sequence+"\n");

            writer.write(">"+proteinAccId);
            writer.newLine();

            // write protein sequence, up to 70 characters per line
            for( int i=0; i<sequence.length(); i+=70 ) {
                int chunkEndPos = i+70;
                if( chunkEndPos > sequence.length() )
                    chunkEndPos = sequence.length();
                String chunk = sequence.substring(i, chunkEndPos);
                writer.write(chunk);
                writer.newLine();
            }
        }

        writer.close();

        getLogWriter().write("DONE! "+nr+" fasta sequences were written out to fasta file!\n");
    }

    public Set<String> readProteinAccIds(int mapKey, String chr) throws IOException {

        // open the polyphen log file
        String polyphenLogFileName = this.getResultsDir()+"/"+mapKey+"."+chr+".log";
        BufferedReader reader = new BufferedReader(new FileReader(polyphenLogFileName));

        // read all log lines from polyphen
        // extract protein acc ids for lines starting with "ERROR: Unable to locate protein entry ";
        Set<String> proteinAccIds = new HashSet<String>();
        final String pattern = "ERROR: Unable to locate protein entry ";
        String line;
        while( (line=reader.readLine())!=null ) {
            if( !line.startsWith(pattern) )
                continue;
            // line starts with pattern -- extract protein acc id
            int accIdStart = pattern.length();
            int accIdEnd = line.indexOf(' ', accIdStart);
            String accId = line.substring(accIdStart, accIdEnd);
            proteinAccIds.add(accId);
        }

        reader.close();
        return proteinAccIds;
    }

    public String getProteinSequence(String proteinAccId) throws Exception {

        for(Transcript transcript: transcriptDAO.getTranscriptsByProteinAccId(proteinAccId) ) {
            for( Sequence seq: sequenceDAO.getObjectSequences(transcript.getRgdId(), "ncbi_protein") ) {
                // process protein sequence
                return seq.getSeqData();
            }
        }
        return null;
    }

    List<String> getChromosomes(int sampleId) throws Exception {

        String sql = "SELECT DISTINCT chromosome FROM variant_map_data WHERE map_key=? ";
        StringListQuery q = new StringListQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        return q.execute(new Object[]{sampleId});
    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public String getResultsDir() {
        return resultsDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }
}
