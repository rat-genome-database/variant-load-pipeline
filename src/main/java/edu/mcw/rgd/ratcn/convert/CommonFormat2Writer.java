package edu.mcw.rgd.ratcn.convert;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.Utils;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.zip.GZIPOutputStream;

/**
 * @author mtutaj
 * @since 10/30/14
 * encapsulates logic to write into a file in common-format-2
 */
public class CommonFormat2Writer {

    private BufferedWriter writer;
    private int mapKey;
    private String dbSnpSource;
    private boolean compressOutputFile;
    private boolean appendToOutputFile;

    static private Connection conn;

    public CommonFormat2Writer(boolean compressOutputFile, boolean appendToOutputFile) {
        this.compressOutputFile = compressOutputFile;
        this.appendToOutputFile = appendToOutputFile;
    }

    /** create a file and write a header
     *
     * @param fileName file name to be created
     */
    public void create(String fileName) throws IOException {

        boolean writeHeader = true;
        if( appendToOutputFile ) {
            if( new File(fileName).exists()  &&  new File(fileName).length()>0 ) {
                writeHeader = false;
            }
        }

        // ensure output dir is created
        File fileNameDir = new File(fileName).getAbsoluteFile().getParentFile();
        fileNameDir.mkdirs();

        if( isCompressOutputFile() ) {
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fileName, appendToOutputFile))));
        } else {
            writer = new BufferedWriter(new FileWriter(fileName, appendToOutputFile));
        }

        // write header
        if( writeHeader ) {
            writer.write("#chr\tposition\tref nuc\tvar nuc\trsId\tA reads\tC reads\tG reads\tT reads\ttotal depth\thgvs name\trgd id\tallele depth\tallele count\tread depth\tpadding base\n");
        }
    }

    /**
     * write a line into common-format-3 file
     * NOTE: if rsId field is null, database table DB_SNP is queried to get this information
     * @param line
     * @throws Exception
     */
    public void writeLine(CommonFormat2Line line) throws Exception {

        if( !line.adjustForIndels() ) {
            return;
        }

        if( line.getRsId()==null ) {
            line.setRsId(getDbSnpRsId(line.getPos(), line.getChr()));
        }
        StringBuilder sb = new StringBuilder();
        sb.append(line.getChr()+"\t")
        .append(line.getChr()+"\t")
        .append(line.getPos()+"\t")
        .append(Utils.defaultString(line.getRefNuc())+"\t")
        .append(Utils.defaultString(line.getVarNuc())+"\t")
        .append(line.getRsId()+"\t");

        // for indels, there are no ACGT counts
        if( line.getRefNuc()==null || line.getVarNuc()==null ) {
            sb.append("\t\t\t\t");
        } else {
            sb.append(getInt(line.getCountA()) + "\t")
            .append(getInt(line.getCountC()) + "\t")
            .append(getInt(line.getCountG()) + "\t")
            .append(getInt(line.getCountT()) + "\t");
        }

        sb.append(getInt(line.getTotalDepth())+"\t")
                .append(Utils.defaultString(line.getHgvsName())+"\t")
        .append(getInt(line.getRgdId())+"\t")
        .append(getInt(line.getAlleleDepth())+"\t")
        .append(getInt(line.getAlleleCount())+"\t")
        .append(getInt(line.getReadDepth())+"\t")
        .append(Utils.defaultString(line.getPaddingBase())+"\n");

        writer.write(sb.toString());
    }

    String getInt(Integer i) {
        if( i==null )
            return "";
        return Integer.toString(i);
    }

    public void close() throws IOException {
        if( writer!=null )
            writer.close();
    }

    static public void closeDbConnection() throws Exception {
        if( conn!=null )
            conn.close();
    }

    static String prevRsId = null;
    static String prevPosChr = "";

    String getDbSnpRsId(int pos, String chr) throws Exception {

        if( dbSnpSource==null ) {
            return "";
        }

        // use caching if possible
        String posChr = pos+"|"+chr;
        if( posChr.equals(prevPosChr) ) {
            return prevRsId;
        }

        // init connection
        if( conn==null )
            conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();

        String sql = "SELECT allele,snp_name FROM db_snp WHERE position=? AND map_key=? AND source=? AND chromosome=?";

        PreparedStatement stmt = conn.prepareStatement(sql);

        stmt.setLong(1, pos);
        stmt.setInt(2, mapKey);
        stmt.setString(3, dbSnpSource);
        stmt.setString(4, chr);

        ResultSet rs = stmt.executeQuery();

        String rsId = "";
        if (rs.next()) {
            rsId = rs.getString("snp_name");
        }

        stmt.close();

        prevPosChr = posChr;
        prevRsId = rsId;

        return rsId;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public String getDbSnpSource() {
        return dbSnpSource;
    }

    public void setDbSnpSource(String dbSnpSource) {
        this.dbSnpSource = dbSnpSource;
    }

    public boolean isCompressOutputFile() {
        return compressOutputFile;
    }

    public void setCompressOutputFile(boolean compressOutputFile) {
        this.compressOutputFile = compressOutputFile;
    }
}
