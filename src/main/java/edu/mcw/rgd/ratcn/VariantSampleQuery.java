package edu.mcw.rgd.ratcn;


import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;


public class VariantSampleQuery extends MappingSqlQuery {

    public VariantSampleQuery(DataSource ds, String query) {
        super(ds, query);
    }
    
    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {

        VariantSampleDetail obj = new VariantSampleDetail();
        obj.setId(rs.getInt("rgd_id"));
        obj.setSource(rs.getString("source"));
        obj.setSampleId(rs.getInt("sample_id"));
        obj.setDepth(rs.getInt("total_depth"));
        obj.setVariantFrequency(rs.getInt("var_freq"));
        obj.setZygosityStatus(rs.getString("zygosity_status"));
        obj.setZygosityPercentRead(rs.getInt("zygosity_percent_read"));
        obj.setZygosityPossibleError(rs.getString("zygosity_poss_error"));
        obj.setZygosityRefAllele(rs.getString("zygosity_ref_allele"));
        obj.setZygosityNumberAllele(rs.getInt("zygosity_num_allele"));
        obj.setZygosityInPseudo(rs.getString("zygosity_in_pseudo"));
        obj.setQualityScore(rs.getInt("quality_score"));

        return obj;
    }
}
