package edu.mcw.rgd.ratcn;


import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;


public class VariantQuery extends MappingSqlQuery {

    public VariantQuery(DataSource ds, String query) {
        super(ds, query);
    }



    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {

        Variant obj = new Variant();
        obj.setId(rs.getLong("rgd_id"));
        obj.setRsId(rs.getString("rs_id"));
        obj.setReferenceNucleotide(rs.getString("ref_nuc"));
        obj.setVariantNucleotide(rs.getString("var_nuc"));
        obj.setVariantType(rs.getString("variant_type"));
        obj.setClinvarId(rs.getString("clinvar_id"));
        obj.setSpeciesTypeKey(rs.getInt("species_type_key"));

        return obj;
    }
}
