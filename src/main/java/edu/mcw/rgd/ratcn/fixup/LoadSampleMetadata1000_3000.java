package edu.mcw.rgd.ratcn.fixup;

import edu.mcw.rgd.dao.DataSourceFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;

public class LoadSampleMetadata1000_3000 {

    public static void main(String[] args) throws Exception {

        DataSource ds = (DataSourceFactory.getInstance().getCarpeNovoDataSource());
        Connection conn = ds.getConnection();
        Statement st = conn.createStatement();

        String file="/tmp/samples1000_3000.txt";
        BufferedReader in = new BufferedReader(new FileReader(file));

        // load columns from header line
        String header = in.readLine();
        String[] hcols = header.split("[\t]");
        String line;

        while( (line=in.readLine())!=null ) {
            if( line.isEmpty() ) {
                continue;
            }
            String[] cols = line.split("[\\t]", -1);

            String sampleId = cols[0];
            if( sampleId.isEmpty() ) {
                continue;
            }

            String sql = null;
            for(int i=1; i<cols.length; i++ ) {
                if( sql==null ) {
                    sql = "UPDATE sample SET ";
                } else {
                    sql += ", ";
                }
                sql += hcols[i] + "='"+cols[i]+"'";
            }
            sql += " WHERE sample_id="+cols[0];

            st.execute(sql);
        }

        conn.close();
        in.close();
    }
}
