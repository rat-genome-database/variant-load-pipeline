package edu.mcw.rgd.ratcn;


import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;


public class VariantMapQuery extends MappingSqlQuery {

    public VariantMapQuery(DataSource ds, String query) {
        super(ds, query);
    }

    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {

        VariantMapData obj = new VariantMapData();
        obj.setId(rs.getInt("rgd_id"));
        obj.setRsId(rs.getString("rs_id"));
        obj.setReferenceNucleotide(rs.getString("ref_nuc"));
        obj.setVariantNucleotide(rs.getString("var_nuc"));
        obj.setVariantType(rs.getString("variant_type"));
        obj.setClinvarId(rs.getString("clinvar_id"));
        obj.setSpeciesTypeKey(rs.getInt("species_type_key"));
        obj.setChromosome(rs.getString("chromosome"));
        obj.setPaddingBase(rs.getString("padding_base"));
        obj.setStartPos(rs.getLong("start_pos"));
        obj.setEndPos(rs.getLong("end_pos"));
        obj.setGenicStatus(rs.getString("genic_status"));
        obj.setMapKey(rs.getInt("map_key"));

        return obj;
    }
}
