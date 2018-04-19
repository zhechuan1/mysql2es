package com.justplay1994.github.mysql2es;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.justplay1994.github.mysql2es.database.DatabaseNode;
import com.justplay1994.github.mysql2es.database.DatabaseNodeListInfo;
import com.justplay1994.github.mysql2es.database.TableNode;
import com.justplay1994.github.mysql2es.es.ESBulkData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.*;

/**
 * Created by JustPlay1994 on 2018/4/3.
 * https://github.com/JustPlay1994/daily-log-manager
 */

/**
 * 批量数据插入
 */
public class Mysql2es {

    public static final Logger logger = LoggerFactory.getLogger(Mysql2es.class);

    public static String ESUrl = "http://192.168.3.250:10000/";
    public static String latStr = "Y";
    public static String lonStr = "X";
    public static int BULKSIZE = 10*1024*1024;/*批量块大小，单位：B*/
    public static int maxThreadCount = 8;/*最大线程数*/
    String driver = "com.mysql.jdbc.Driver";
    String URL = "jdbc:mysql://localhost:3306/";
    String USER = "root";
    String PASSWORD = "123456";

//    public  static List<DatabaseNode> databaseNodeList;/*所有数据*/
//    public static int dbNumber=0;/*数据库总数量*/
//    public static int tbNumber = 0;/*表总数量*/
//    public static long rowNumber=0;/*总数据量*/

    /*跳过的数据库的集合*/
    public static String[] skipDB = {"information_schema","mysql","performance_schema","sys"};
    /*跳过表的集合*/
    public static String[] skipTB;
    /*必须读取库的集合*/
    public static String[] justReadDB;
    /*必须读取表的集合*/
    public static String[] justReadTB;

    Properties properties = new Properties();/*Mysql相关属性*/

    /**
     * 索引与库表名的关系映射
     * @param dbName
     * @param tbName
     * @return 索引名称
     */
    public static String indexName(String dbName,String tbName){
        return tbName+"@"+dbName;
    }


    public static void main(String[] args){
        logger.info("start copy data from mysql to es ...");
        Mysql2es mysql2es = new Mysql2es();
        mysql2es.doPerHour();
    }
    public Mysql2es(){
        InputStream inputStream =this.getClass().getResourceAsStream("/mysql2es.properties");

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error("读取配置文件失败",e);
        }
        ESUrl = properties.getProperty("ESUrl");
        latStr = (String) properties.get("latStr");
        lonStr = (String) properties.get("lonStr");
        BULKSIZE = Integer.parseInt(properties.get("BULKSIZE").toString())*1024*1024;
        maxThreadCount = Integer.parseInt(properties.get("maxThreadCount").toString());
        this.driver = (String) properties.get("driver");
        this.URL = (String) properties.get("URL");
        this.USER = (String) properties.get("USER");
        this.PASSWORD = (String) properties.get("PASSWORD");
        skipDB = properties.get("skipDB")!=null ? ((String)properties.get("skipDB")).replace(" ","").split(","):skipDB;
        skipTB = properties.get("skipTB")!=null ? ((String)properties.get("skipTB")).replace(" ","").split(","):null;
        justReadDB = properties.get("justReadDB")!=null ? ((String)properties.get("justReadDB")).replace(" ","").split(","):null;
        justReadTB = properties.get("justReadTB")!=null ? ((String)properties.get("justReadTB")).replace(" ","").split(","):null;

        /*初始化Mysql属性*/
        properties.setProperty("user",USER);
        properties.setProperty("password",PASSWORD);
        properties.setProperty("useSSL","false");
        properties.setProperty("verifyServerCertificate","false");
        /*
        SQL默认date的值为0000-00-00，
        但 java.sql.Date 将其视为 不合法的值 格式不正确，
        然后读取的时候会报错，需要加上该属性，将date默认值转换为null
        参考 https://blog.csdn.net/ja_II_ck/article/details/3905120
        */
        properties.setProperty("zeroDateTimeBehavior","convertToNull");
    }





    public void doPerHour(){
        logger.info("delete es all data...");
        esDeleteAll();
        logger.info("delete finished!");



        /*注册驱动，那么在多线程，多种驱动的情况下，会发生什么？
        * 按照博客http://hllvm.group.iteye.com/group/topic/39251
        * 的说法，Class.forName会被阻塞
        * */
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

            /*获取所有表结构*/
            getAllDatabaseStructure();

            /*获取所有数据*/
            getAllData();

            /*打印获取的数据总量情况*/
            logger.info("data is all in memory!");
            logger.info("========================");
            logger.info("dbNumber: "+ DatabaseNodeListInfo.dbNumber);
            logger.info("tbNumber: "+ DatabaseNodeListInfo.tbNumber);
            logger.info("rowNumber: "+ DatabaseNodeListInfo.rowNumber);
            logger.info("========================");

            /*开始导入数据至es中*/
            new ESBulkData(ESUrl, DatabaseNodeListInfo.databaseNodeList).inputData();

        }
        catch(SQLException e)
        {
            logger.error("mysql error",e);
        }
    }

    /**
     * 获取所有库表结构，保存至databaseNodeList
     * @return
     * @throws SQLException
     */
    public void getAllDatabaseStructure() throws SQLException {
        /*连接Mysql相关变量*/
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        String sql = "select * from ";

        /*查询所有库、表、字段*/
        con= DriverManager.getConnection(URL + "information_schema", properties);

        logger.info("Connect mysql Successfull.");

        st=con.createStatement();

//            st.setString(1,"tb_person_time");
//            rs  = st.executeQuery();
        rs = st.executeQuery(sql+"COLUMNS");

            /*获取所有库、表、列名开始*/
        DatabaseNodeListInfo.databaseNodeList = new ArrayList<DatabaseNode>();

        DatabaseNode lastDB = null;
        TableNode lastTable = null;
        while(rs.next()){
            String colStr = rs.getString("COLUMN_NAME");
            String tbStr = rs.getString("TABLE_NAME");
            String dbStr = rs.getString("TABLE_SCHEMA");

            boolean skip = false;
            /*判断该库是否是必须读取*/
            if(justReadDB!=null){
                skip = true;
                for(int i = 0; i < justReadDB.length; ++i){
                    if(dbStr.equals(justReadDB[i])){
                        skip = false;
                        break;
                    }
                }
            }
            /*判断该表是否是必须读取*/
            if(justReadTB!=null){
                skip = true;
                for(int i = 0; i < justReadTB.length; ++i){
                    if(dbStr.equals(justReadTB[i].split("\\.")[0]) && tbStr.equals(justReadTB[i].split("\\.")[1])){
                        skip = false;
                        break;
                    }
                }
            }
            /*判断该库是否需要跳过*/
            if(skipDB!=null) {
                for (int i = 0; i < skipDB.length; ++i) {
                    if (dbStr.equals(skipDB[i])) {
                        skip = true;
                        break;
                    }
                }
            }
            /*判断该表是否需要跳过*/
            if(skipTB!=null) {
                for (int i = 0; i < skipTB.length; ++i) {
                    if (dbStr.equals(skipTB[i].split("\\.")[0]) && tbStr.equals(skipTB[i].split("\\.")[1])) {
                        skip = true;
                        break;
                    }
                }
            }

            if(skip)continue;

            if (lastDB==null){
                lastDB = new DatabaseNode(dbStr,new ArrayList<TableNode>());
                lastTable =null;
                DatabaseNodeListInfo.databaseNodeList.add(lastDB);
            }else{
                if(!dbStr.equals(lastDB.getDbName())){
                    lastDB = new DatabaseNode(dbStr,new ArrayList<TableNode>());
                    lastTable =null;
                    DatabaseNodeListInfo.databaseNodeList.add(lastDB);
                }
            }
            if(lastTable==null){
                lastTable = new TableNode(tbStr, new ArrayList<String>(), new ArrayList<ArrayList<String>>());
                lastTable.getColumns().add(colStr);
                lastDB.getTableNodeList().add(lastTable);
            }else{
                if(!tbStr.equals(lastTable.getTableName())){
                    lastTable = new TableNode(tbStr, new ArrayList<String>(), new ArrayList<ArrayList<String>>());
                    lastTable.getColumns().add(colStr);
                    lastDB.getTableNodeList().add(lastTable);
                }else{
                    lastTable.getColumns().add(colStr);
                }
            }
        }
        /*获取所有库、表、列名结束*/
        /*TODO 这里有多次连接，需要每次创建新的之前，都close之前的，还是只需在最后close即可*/
        rs.close();
        st.close();
        con.close();
    }

    /**
     * 遍历表结构，获取所有数据，保存至databaseNodeList
     * @throws SQLException
     */
    public void getAllData() throws SQLException {
        /*连接Mysql相关变量*/
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        String sql = "select * from ";

        Iterator<DatabaseNode> databaseNodeIt = DatabaseNodeListInfo.databaseNodeList.iterator();
        while(databaseNodeIt.hasNext()){
            DatabaseNodeListInfo.dbNumber++;
            DatabaseNode databaseNode = databaseNodeIt.next();
                /*获取数据库连接*/
            con = DriverManager.getConnection(URL+databaseNode.getDbName(),properties);
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
            while(tableNodeIterator.hasNext()){
                DatabaseNodeListInfo.tbNumber++;
                TableNode tableNode = tableNodeIterator.next();
                    /*sql查询该表所有数据*/
                st=con.createStatement();
                rs = st.executeQuery(sql+tableNode.getTableName());
                while(rs.next()){
                    DatabaseNodeListInfo.rowNumber++;
                    ResultSetMetaData md = rs.getMetaData();
                    int columnCount = md.getColumnCount();
                    ArrayList<String> row = new ArrayList<String>();
                    for(int i = 1; i <= columnCount; ++i) {
                        row.add(rs.getString(i));
                    }
                    tableNode.getRows().add(row);
                }
            }
        }
        /*TODO 这里有多次连接，需要每次创建新的之前，都close之前的，还是只需在最后close即可*/
        rs.close();
        st.close();
        con.close();
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
            if(row.get(latStr)!=null && row.get(lonStr)!=null) {
                HashMap location = new HashMap();
                location.put("type", "geo_point"); //点
//            location.put("type","geo_shape");     //面
                propertiesMap.put("location", location);
            }
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

    public void esDeleteAll(){
        logger.info("esMapping ...");
        try {

            URL url = new URL(ESUrl+"_all");
            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection)urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type","application/json");
            httpURLConnection.setRequestMethod("DELETE");
            httpURLConnection.setConnectTimeout(3000);

            httpURLConnection.connect();

            InputStream inputStream = httpURLConnection.getInputStream();
//            System.out.println(inputStream);
            httpURLConnection.disconnect();

//            new MyURLConnection().request("ESUrl+_all","DELETE","");


        } catch (MalformedURLException e) {
            logger.error("error",e);
        } catch (IOException e) {
            logger.error("error",e);
        }
    }

}
