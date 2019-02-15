package com.example.oraceltoclickhouse.read;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Component
public class OperateOracle implements CommandLineRunner {


    // 定义连接所需的字符串
    // 192.168.0.X是本机地址(要改成自己的IP地址)，1521端口号，XE是精简版Oracle的默认数据库名
    @Value("${oracleUser}")
    private String USERNAMR;
    @Value("${oraclePwd}")
    private String PASSWORD;
    private static  String DRVIER = "oracle.jdbc.OracleDriver";
    @Value("${oracleUrl}")
    private String URL;

    @Value("${datadir}")
    private String datadir;

    @Value("${database}")
    private String database;

    @Value("${tablename}")
    private String tablename;

    // 创建一个数据库连接
    Connection connection = null;
    // 创建预编译语句对象，一般都是用这个而不用Statement
    //PreparedStatement pstm = null;
    // 创建一个结果集对象
    //ResultSet rs = null;

    private Map<String,ArrayList> TableMap;

    public void init(){
        String databaseName = database;
        File file1=new File(datadir+"/"+databaseName.toLowerCase());
        file1.mkdirs();
        GetTableList();
    }

    public String initDir(String dir){
        String databaseName = database;
        String path = datadir+"/"+databaseName.toLowerCase()+"/"+dir;
        File file1=new File(path);
        file1.mkdir();
        return path;
    }

    @Override
    public void run(String... args) throws Exception {
        init();
    }

    public class ColumnType {
        public String ColumnName;
        public String DataType;
        public boolean Nullable;
    }

    public void  WriteStruct(String table,String sql,String type){
        String databaseName = database;
        try {
            String path = datadir + "/" + databaseName.toLowerCase() +  "/" + table.toLowerCase() + "/" + table.toLowerCase() + type;
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWritter = new FileWriter(file);
            fileWritter.write(sql);
            fileWritter.close();
            //System.out.println(sql);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void  WriteExportDataSQL(String table,String sql){
        String databaseName = database;
        try {
            String path = datadir + "/" + databaseName.toLowerCase()  + "/" + table.toLowerCase() + "/" + table.toLowerCase() + ".export.sql";
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWritter = new FileWriter(file);
            fileWritter.write(sql);
            fileWritter.close();
           // System.out.println(sql);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 获取库里的所有表，并放在List里
     */
    public void GetTableList() {
        TableMap = new HashMap<String,ArrayList>();
        connection = getConnection();
        String sql = "select table_name from dba_tables where owner='"+database.toUpperCase()+"'";

        if (tablename != null && !tablename.equals("")){
            sql += " AND table_name='"+tablename.toUpperCase()+"'";
        }
        System.out.println(sql);
        try {
            Statement stmt = connection.createStatement();
            //pstm = connection.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String table_name = rs.getString("table_name");
                initDir(table_name.toLowerCase());
                TableMap.put(table_name,null);
                exportDBStruct(database,table_name);
            }
            stmt.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //ReleaseResource();
        }
    }

    public String getTableIndex(String tablename){
        connection = getConnection();
        String sql = "select COLUMN_NAME from all_ind_columns where index_owner = '"+database.toUpperCase()+"' and TABLE_NAME='"+tablename.toUpperCase()+"' GROUP BY COLUMN_NAME";
        Statement stmt;
        ResultSet rs;
        String Index = "";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                if(Index.equals("")){
                    Index = rs.getString("COLUMN_NAME");
                }else{
                    Index +=","+rs.getString("COLUMN_NAME");
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
        return Index;
    }


    public boolean exportDBStruct(String database,String tableName){
        connection = getConnection();
        String ddlSql = "";
        String sql = "SELECT OWNER,table_name,column_name,data_type,data_precision,data_scale,nullable,column_id FROM  all_tab_columns where owner = '"+database.toUpperCase()+"' and table_name = '"+tableName.toUpperCase()+"' order by column_id asc";
        Statement stmt;
        ResultSet rs;
        database = database.toLowerCase();
        tableName = tableName.toLowerCase();
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            String EventDateColumn = "";
            ArrayList<ColumnType> ConlumnList = new ArrayList<>();
            while (rs.next()) {
                ColumnType ct = new ColumnType();
                String column_name = rs.getString("column_name").toLowerCase();
                String data_type = rs.getString("data_type");
                int data_precision = rs.getInt("data_precision");
                //int data_scale = rs.getInt("data_scale");
                String nullable = rs.getString("nullable");

                String DataType;
                switch (data_type){
                    case "NUMBER":
                        if (data_precision > 0){
                            if (data_precision>9){
                                DataType = "Float64";
                            }else{
                                DataType = "Float32";
                            }
                        }else{
                            DataType = "Int64";
                        }
                        break;
                    case "DATE":
                        if (EventDateColumn.equals("")){
                            EventDateColumn = column_name;
                        }
                        DataType = "DateTime";
                        break;
                    default:
                        DataType = "String";
                        break;
                }
                ct.DataType = DataType;
                if (nullable == "Y"){
                    ct.Nullable = true;
                    //DataType = "Nullable("+DataType+")";
                }else{
                    ct.Nullable = false;
                }
                ct.ColumnName = column_name;
                if (ddlSql == ""){
                    ddlSql += column_name+" "+DataType;
                }else{
                    ddlSql += ","+column_name+" "+DataType;
                }
                ConlumnList.add(ct);
            }

            String SQL;

            SQL = "CREATE TABLE "+database+"."+tableName+"("+ddlSql+") ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/"+database+"/"+tableName+"', '{replica}') ";

            if (!EventDateColumn.equals("")){
                //ddlSql += " PARTITION BY toYYYYMM("+EventDateColumn+")";
            }
            String Index = getTableIndex(tableName);
            if(!Index.equals("")){
                SQL += " ORDER BY ("+Index+")";
            }
            System.out.println(SQL);
            WriteStruct(tableName,SQL,".frm.sql");

            SQL = "CREATE TABLE "+database+"."+tableName+"_all"+"("+ddlSql+") ENGINE = Distributed(bip_ck_cluster, '"+database+"', '"+tableName+"', rand())";
            System.out.println(SQL);
            WriteStruct(tableName,SQL,".Distributed.frm.sql");
            TableMap.put(tableName,ConlumnList);
            exportDataFromTableSql(tableName,ConlumnList);
            stmt.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
        return true;
    }
    public void exportDataFromTableSql(String talbeName ,ArrayList<ColumnType> ColumnList){
        String Values = "";
        for(int i=0;i<ColumnList.size();i++){
            String ColumnName = ColumnList.get(i).ColumnName;
            String DataType = ColumnList.get(i).DataType;
            boolean Nullable = ColumnList.get(i).Nullable;
            String v = "";
            switch (DataType){
                case "DateTime":
                    //Nullable = true;
                    v = "case when "+ColumnName+" is not null then CASE when etl.unix_timestamp("+ColumnName+", 8) > 0 THEN to_char(to_char(etl.unix_timestamp("+ColumnName+", 8))) ELSE '-28800' END else '-28800' end";
                    //v = "etl.unix_timestamp("+ColumnName+", 8)";
                    break;
                default:
                    if (Nullable == true)
                    {
                        if (DataType.equals("String")){
                            v = "case when "+ColumnName+" is not null then to_char("+ColumnName+") else '' end";
                        }else{
                            v = "case when "+ColumnName+" is not null then to_char("+ColumnName+") else '0' end";
                        }
                    }else{
                        v = ColumnName;
                    }

                    break;
            }
            /*
            if (Nullable == true){
                v = "case when "+ColumnName+" is not null then to_char("+v+") else '\\N' end";
            }
            */

            if (i==0){
                Values = v;
            }else{
                Values += ","+v;
            }
        }
        String Sql = "SELECT "+Values+" FROM "+database+"."+talbeName;
        WriteExportDataSQL(talbeName,Sql);
    }


    /**
     * 获取Connection对象
     *
     * @return
     */
    public Connection getConnection() {
        try {
            if (connection != null){
                return connection;
            }
            Class.forName(DRVIER);
            connection = DriverManager.getConnection(URL, USERNAMR, PASSWORD);
            System.out.println(connection.getMetaData());
            System.out.println("成功连接数据库");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not find !", e);
        } catch (SQLException e) {
            throw new RuntimeException("get connection error!", e);
        }

        return connection;
    }

    /**
     * 释放资源
     */
    /*
    public void ReleaseResource() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    */
}