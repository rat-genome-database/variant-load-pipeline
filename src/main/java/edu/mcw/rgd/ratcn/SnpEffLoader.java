package edu.mcw.rgd.ratcn;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.process.Utils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mtutaj on 4/18/2017.
 */
public class SnpEffLoader {

    private String version;

    Dao dao = new Dao();
    VariantDAO vdao = new VariantDAO();

    public SnpEffLoader() throws Exception {
        vdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
    }

    public static void main(String[] args) throws Exception {

        long time0 = System.currentTimeMillis();

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        SnpEffLoader instance = (SnpEffLoader) (bf.getBean("snpEffLoader"));

        System.out.println(instance.getVersion());

        try {
            instance.run(args);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }

        System.out.println("=== OK === elapsed "+ Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    public void run(String[] args) throws Exception {

        String inputFile = "/rgd/SR_snpEff_rn5_filter_missense.vcf";
        int sampleId = 726;

        System.out.println("INPUT_FILE "+inputFile);
        System.out.println("SAMPLE_ID  "+sampleId);

        // reset counters
        int variantsProcessed = 0;
        int variantsInDb = 0;
        int snpEffRowsUpdated = 0;
        int snpEffRowsInserted = 0;


        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;
        while( (line=reader.readLine())!=null ) {
            // skip comment lines
            if( line.startsWith("#") || line.isEmpty() ) {
                continue;
            }
            // parse columns
            // columns: CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO ...
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<8 ) {
                continue;
            }
            variantsProcessed++;

            String chr = cols[0];
            int pos = Integer.parseInt(cols[1]);
            String refNuc = cols[3];
            String varNuc = cols[4];
            String info = cols[7];

            // find matching variant in db
            List<Variant> variants = getVariantsInDb(sampleId, chr, pos, refNuc, varNuc);
            if( variants.isEmpty() ) {
                continue;
            }
            variantsInDb++;

            // parse SnpEff data
            List<SnpEff> snpEffs = parseSnpEffData(info);
            for( SnpEff snpEff: snpEffs ) {
                for( Variant v: variants ) {
                    if( upsertSnpEffInDb(v.getId(), snpEff) ) {
                        snpEffRowsInserted++;
                    } else {
                        snpEffRowsUpdated++;
                    }
                }
            }
        }
        reader.close();

        System.out.println("variants processed "+variantsProcessed);
        System.out.println("variants in db "+variantsInDb);
        System.out.println("SnpEff rows updated "+snpEffRowsUpdated);
        System.out.println("SnpEff rows inserted "+snpEffRowsInserted);
    }

    List<Variant> getVariantsInDb(int sampleId, String chr, int pos, String refNuc, String varNuc) {

        List<Variant> variants = vdao.getVariants(sampleId, chr, pos, pos);
        Iterator<Variant> it = variants.iterator();
        while( it.hasNext() ) {
            Variant v = it.next();
            if( !v.getReferenceNucleotide().equals(refNuc) || !v.getVariantNucleotide().equals(varNuc) ) {
                it.remove();
            }
        }
        return variants;
    }

    List<SnpEff> parseSnpEffData(String vcfInfo) {

        List<SnpEff> snpEffs = new ArrayList<>();

        // extract SnpEff annotation from vcf INFO field
        int pos1 = vcfInfo.indexOf("ANN=");
        int pos2 = vcfInfo.indexOf(';', pos1);
        String annotStr = pos1<0 ? null : pos2>0 ? vcfInfo.substring(pos1+4, pos2) : vcfInfo.substring(pos1+4);
        String[] annots = annotStr.split("[,]");
        for( String annot: annots ) {
            String[] fields = annot.split("[\\|]", -1);
            SnpEff snpEff = new SnpEff();
            snpEff.allele = nullIfEmpty(fields[0]);
            snpEff.effect = nullIfEmpty(fields[1]);
            snpEff.impact = nullIfEmpty(fields[2]);
            snpEff.geneSymbol = nullIfEmpty(fields[3]);
            snpEff.geneId = nullIfEmpty(fields[4]);
            snpEff.feature = nullIfEmpty(fields[5]);
            snpEff.featureId = nullIfEmpty(fields[6]);
            snpEff.bioType = nullIfEmpty(fields[7]);
            snpEff.rank = nullIfEmpty(fields[8]);
            snpEff.hgvsC = nullIfEmpty(fields[9]);
            snpEff.hgvsP = nullIfEmpty(fields[10]);
            snpEff.cdnaPosLen = nullIfEmpty(fields[11]);
            snpEff.cdsPosLen = nullIfEmpty(fields[12]);
            snpEff.aaPosLen = nullIfEmpty(fields[13]);
            snpEff.distance = nullIfEmpty(fields[14]);
            snpEff.errors = nullIfEmpty(fields[15]);

            snpEffs.add(snpEff);
        }

        return snpEffs;
    }

    boolean upsertSnpEffInDb(long varId, SnpEff snpEff) throws Exception {

        String sql = "INSERT INTO snp_eff (snp_eff_id,variant_id,allele,effect,impact,gene_symbol,gene_id,feature,"+
                "feature_id,biotype,rank,hgvs_c,hgvs_p,cdna_pos_len,cds_pos_len,aa_pos_len,distance,errors) "+
                "VALUES(snp_eff_seq.NEXTVAL,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?)";
        dao.update(sql, varId, snpEff.allele, snpEff.effect, snpEff.impact, snpEff.geneSymbol, snpEff.geneId,
                snpEff.feature, snpEff.featureId, snpEff.bioType, snpEff.rank, snpEff.hgvsC, snpEff.hgvsP,
                snpEff.cdnaPosLen, snpEff.cdsPosLen, snpEff.aaPosLen, snpEff.distance, snpEff.errors);
        return true; // inserted
    }

    String nullIfEmpty(String s) {
        return s.isEmpty() ? null : s;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    class SnpEff {
        public int snpEffId;
        public long variantId;
        public String allele;
        public String effect;
        public String impact;
        public String geneSymbol;
        public String geneId;
        public String feature;
        public String featureId;
        public String bioType;
        public String rank;
        public String hgvsC;
        public String hgvsP;
        public String cdnaPosLen;
        public String cdsPosLen;
        public String aaPosLen;
        public String distance;
        public String errors;
        public Date createdDate;
    }

    class Dao extends AbstractDAO {

        DataSource ds;

        public DataSource getDataSource() throws Exception {
            if( ds==null ) {
                ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
            }
            return ds;
        }
    }
}
