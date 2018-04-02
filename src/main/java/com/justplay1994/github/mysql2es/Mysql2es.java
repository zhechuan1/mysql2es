package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * 同步所有库，除mysql系统库：information_schema、mysql、performance_schema
 * 库名对应es的索引，表名对应type，一行数据对应一个id
 */
public class Mysql2es{

    public static final Logger logger = LoggerFactory.getLogger(Mysql2es.class);
    public static void main(String[] args){
        logger.info("start copy data from mysql to es ...");
        Mysql2es mysql2es = new Mysql2es();
        mysql2es.doPerHour();

    }
    public static String ESUrl = "http://192.168.3.250:10000/";
    public static String latStr = "Y";
    public static String lonStr = "X";

    public void doPerHour(){
        String driver = "com.mysql.jdbc.Driver";
        String URL = "jdbc:mysql://localhost:3306/";
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "select * from ";
        String USERNAME = "root";
        String PASSWORD = "123456";

        String[] skipDB = {"information_schema","mysql","performance_schema"};

        int dbNum = 0;
        int tbNum = 0;
        int colNum = 0;
        int rowNum = 0;

        Properties properties = new Properties();
        properties.setProperty("useSSL","false");
        properties.setProperty("verifyServerCertificate","false");

        try
        {
            Class.forName(driver);
        }
        catch(java.lang.ClassNotFoundException e)
        {
            logger.error("Cant't load Driver");
        }
        try
        {
            /*查询所有库、表、字段*/
            con= DriverManager.getConnection(URL+"information_schema",USERNAME,PASSWORD);
            logger.info("Connect mysql Successfull.");

            st=con.createStatement();

//            st.setString(1,"tb_person_time");
//            rs  = st.executeQuery();
            rs = st.executeQuery(sql+"COLUMNS");

            HashMap<String,HashMap<String,ArrayList<String>>> dbs = new HashMap<String, HashMap<String, ArrayList<String>>>();

            while(rs.next()){

                String colStr = rs.getString("COLUMN_NAME");
                String tbStr = rs.getString("TABLE_NAME");
                String dbStr = rs.getString("TABLE_SCHEMA");
                if(dbs.get(dbStr)==null){
                    dbs.put(dbStr,new HashMap<String, ArrayList<String>>());
                }
                if(dbs.get(dbStr).get(tbStr)==null){
                    dbs.get(dbStr).put(tbStr,new ArrayList<String>());
                }
                dbs.get(dbStr).get(tbStr).add(colStr);
            }

            Set<String> dbSet = dbs.keySet();
            Iterator<String> dbIt = dbSet.iterator();
            while(dbIt.hasNext()){
                dbNum++;
                String dbName = dbIt.next();
                boolean skip = false;
                for(int s = 0; s < skipDB.length; ++s){
                    if(skipDB[s].equals(dbName)){
                        skip=true;
                        break;
                    }
                }
                if(skip)continue;
                con = DriverManager.getConnection(URL+dbName,USERNAME,PASSWORD);
                st = con.createStatement();
                Set<String> tbSet = dbs.get(dbName).keySet();
                Iterator<String> tbIt = tbSet.iterator();
                while(tbIt.hasNext()) {
                    tbNum++;
                    String tbName = tbIt.next();
                    rs = st.executeQuery(sql +tbName);
                    /*将数据逐条存入es中*/
                    while(rs.next()){
                        rowNum++;
                        HashMap<String, Object> row = new HashMap<String, Object>();
                        for(int i = 0; i < dbs.get(dbName).get(tbName).size();++i) {
                            colNum++;
                            String colName = dbs.get(dbName).get(tbName).get(i);
                            String value = rs.getString(colName);
//                            System.out.println("【"+dbName+"】【"+tbName+"】【"+colName+"】"+value);
//                            if(value!=null)value = URLEncoder.encode(value,"UTF-8");
//                            row.put(tbName+"__"+colName,value);/*es相同索引，即便不同type也不允许有同名字段，因为是扁平化存储*/
                            row.put(colName,value);
                        }
                        logger.info("【"+dbName+"】【"+tbName+"】");
                        logger.info(row.toString());


//                        /*地理坐标点添加至location中*/
                        HashMap location = new HashMap();
                        /*点point*/
                        location.put("lat",row.get(latStr));
                        location.put("lon",row.get(lonStr));
                        /*面*/
//                        location.put("type", "point");
//                        String[] point = {(String)row.get(lonStr),(String)row.get(latStr)};
//                        location.put("coordinates",point);


                        row.put("location",location);

                        if(rs.isFirst()) {
                            esMapping(dbName, tbName, row);
//                            new ESCreateMappingThread(ESUrl,dbName,tbName,row).run();
                        }
//                        es(dbName,tbName,rs.getString("id"),row);
                        new ESInsertDataThread(ESUrl,dbName,tbName,rs.getString("id"),row).run();
                    }
                }
            }

            logger.info("Finished!");
            logger.info("db number: "+dbNum);
            logger.info("tb nubmer: "+tbNum);
            logger.info("row number: "+rowNum);
            logger.info("colum number: "+colNum);

            rs.close();
            st.close();
            con.close();
        }
        catch(Exception e)
        {
            logger.error("error",e);
        }
    }

    /**
     * es6.0已经移除了type
     * @param dbName
     * @param tbName
     * @param id
     * @param row
     */
    public void es(String dbName, String tbName, String id, Map<String,Object> row){
        logger.info("es ...");
        try {
            /*es索引要求必须是小写*/
            dbName = dbName.toLowerCase();
            tbName = tbName.toLowerCase();

            URL url = new URL(ESUrl+dbName+"@"+tbName+"/_doc/"+id);
            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection)urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type","application/json");
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setConnectTimeout(3000);

            httpURLConnection.connect();

            OutputStream outputStream = httpURLConnection.getOutputStream();

            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(row);
            logger.info(json);
            outputStream.write(json.getBytes());

            InputStream inputStream = httpURLConnection.getInputStream();
//            System.out.println(inputStream);

            httpURLConnection.disconnect();
        } catch (MalformedURLException e) {
            logger.error("error",e);
        } catch (IOException e) {
            logger.error("error",e);
        }
    }

    /**
     * 新建索引和映射
     * 每个字段增加data_detection:false； 关闭自动转化为时间格式的功能。
     * 所有的字段映射为text，经纬度映射为geo。防止数据字段映射为date，数据不一致（脏数据）会报错。
     * 只能采用put请求
     * es7.0的index不支持冒号，所以使用@隔开库与表
     * @param dbName
     * @param tbName
     * @param row
     */
    public void esMapping(String dbName, String tbName, Map<String,Object> row){
        logger.info("esMapping ...");
        try {
            /*es索引要求必须是小写*/
            dbName = dbName.toLowerCase();
            tbName = tbName.toLowerCase();

            HashMap propertiesMap = new HashMap();
            Set<String> set = row.keySet();
            Iterator<String> iterator = set.iterator();


            /*映射基础字段*/
            while(iterator.hasNext()){
                HashMap temp = new HashMap();
                String key = iterator.next();
                temp.put("type", "text");
                propertiesMap.put(key,temp);
            }
            /*增加地理信息字段*/
            HashMap location = new HashMap();
            location.put("type","geo_point"); //点
//            location.put("type","geo_shape");     //面
            propertiesMap.put("location",location);

            HashMap indexMap = new HashMap();
            HashMap mappingsMap = new HashMap();
            HashMap  docMap= new HashMap();

            docMap.put("properties",propertiesMap);
            mappingsMap.put("_doc",docMap);
            indexMap.put("mappings",mappingsMap);

            URL url = new URL(ESUrl+dbName+"@"+tbName);
            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection)urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type","application/json");
            httpURLConnection.setRequestMethod("PUT");
            httpURLConnection.setConnectTimeout(3000);

            httpURLConnection.connect();

            OutputStream outputStream = httpURLConnection.getOutputStream();

            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(indexMap);
            logger.info(json);
            outputStream.write(json.getBytes());

            InputStream inputStream = httpURLConnection.getInputStream();
//            System.out.println(inputStream);

            httpURLConnection.disconnect();
        } catch (MalformedURLException e) {
            logger.error("error",e);
        } catch (IOException e) {
            logger.error("error",e);
        }
    }
}
