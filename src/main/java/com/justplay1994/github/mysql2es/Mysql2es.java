package com.justplay1994.github.mysql2es;


import ch.qos.logback.core.joran.spi.JoranException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.justplay1994.github.mysql2es.config.LogBackConfigLoader;
import com.justplay1994.github.mysql2es.database.DatabaseNode;
import com.justplay1994.github.mysql2es.database.DatabaseNodeListInfo;
import com.justplay1994.github.mysql2es.database.TableNode;
import com.justplay1994.github.mysql2es.es.ESBulkData;
import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

    public static String DBTYPE = "";

    public static final String ORACLE = "ORACLE";
    public static final String MYSQL = "MYSQL";

    public static String ESUrl = "http://192.168.3.250:10000/";
    public static String latStr = "Y";
    public static String lonStr = "X";
    public static int BULKSIZE = 10*1024*1024;/*批量块大小，单位：B*/
    public static int maxThreadCount = 8;/*最大线程数*/
    static String driver = "com.mysql.jdbc.Driver";
    static String driverUrl = "jdbc:mysql://localhost:3306/";
    static String user = "root";
    static String password = "1";
    static String justDictionary="false";/*仅仅执行数据字段生成操作，不导入数据*/
	public static String indexType="_doc"; //默认索引的type类型

    public static String OWNER="";/*ORACLE 的owner值*/

    public static String DateTime="false";/*是否要识别时间字段*/

    public static String indexDB="";/*索引使用的库名*/

//    public  static List<DatabaseNode> databaseNodeList;/*所有数据*/
//    public static int dbNumber=0;/*数据库总数量*/
//    public static int tbNumber = 0;/*表总数量*/
//    public static long rowNumber=0;/*总数据量*/

    /*跳过的数据库的集合，TODO Oracle需要进行更改*/
    public static String[] skipDB = {"information_schema","mysql","performance_schema","sys"};
    /*跳过表的集合*/
    public static String[] skipTB;
    /*必须读取库的集合*/
    public static String[] justReadDB;
    /*必须读取表的集合*/
    public static String[] justReadTB;

    private static String dataDictionaryPath="dataDictionary.properties";/*数据字典的路径*/

    static Properties properties = new Properties();/*该实例用于读取配置文件*/

    /**
     * 索引名与库表名的关系映射
     * @param dbName
     * @param tbName
     * @return 索引名称
     */
    public static String indexName(String dbName,String tbName){
        if (!indexDB.equals(""))
            return (tbName+"@"+indexDB).toLowerCase();
        return (tbName+"@"+dbName).toLowerCase();
    }


    public static void main(String[] args){
		long start = System.currentTimeMillis();
        logger.info("Start copy data from mysql to es ...");
        Mysql2es mysql2es = new Mysql2es(args);
        mysql2es.doInput();
        logger.info("Finished copy data from mysql to es");
		long end = System.currentTimeMillis();
		long minute = (end-start)/(1000*60);
		long second = ((end-start)/1000)%60;
		logger.info("total time:"+minute+"m:"+second+"s");
    }
    public Mysql2es(String[] args){
		File f = null;
        InputStream inputStream=null;

        try {
            if (args!=null && args.length>0) {
                String path = args[0];
                System.out.println(path);
                f = new File(path);
                inputStream = new FileInputStream(f);

                if (args.length>1)
                    LogBackConfigLoader.load(args[1]);
            }else{
                inputStream =this.getClass().getResourceAsStream("/mysql2es.properties");
            }
            properties.load(new InputStreamReader(inputStream,"UTF-8"));
        } catch (IOException e) {
            logger.error("读取配置文件失败",e);
        } catch (JoranException e) {
            logger.error("logback read xml error! path: "+args[1],e);
        }
        indexDB=(String)properties.get("indexDB");
        DateTime=(String)properties.get("DateTime");
        justDictionary= (String) properties.get("justDictionary");
        ESUrl = (String)properties.getProperty("ESUrl");
		indexType = (String)properties.getProperty("indexType");
        latStr = (String) properties.get("latStr");
        lonStr = (String) properties.get("lonStr");
        BULKSIZE = Integer.parseInt(properties.get("BULKSIZE").toString())*1024*1024;
        maxThreadCount = Integer.parseInt(properties.get("maxThreadCount").toString());
        this.driver = (String) properties.get("driver");
        this.driverUrl = (String) properties.get("URL");
        this.user = (String) properties.get("USER");
        this.password = (String) properties.get("PASSWORD");
        int num  = skipDB.length;
//        skipDB = properties.get("skipDB")!=null ? ((String)properties.get("skipDB")).replace(" ","").split(","):skipDB;
        if((String)properties.get("skipDB")!=null) {
            String[] mySkipDB = ((String) properties.get("skipDB")).replace(" ", "").split(",");
            String[] _skipDB = new String[skipDB.length + mySkipDB.length];/*扩容*/
            System.arraycopy(skipDB, 0, _skipDB, 0, skipDB.length);
            System.arraycopy(mySkipDB, 0, _skipDB, skipDB.length, mySkipDB.length);
            skipDB = _skipDB;
        }

        skipTB = properties.get("skipTB")!=null ? ((String)properties.get("skipTB")).replace(" ","").split(","):null;
        justReadDB = properties.get("justReadDB")!=null ? ((String)properties.get("justReadDB")).replace(" ","").split(","):null;
        justReadTB = properties.get("justReadTB")!=null ? ((String)properties.get("justReadTB")).replace(" ","").split(","):null;
		
        /*初始化Mysql属性*/
        if (driver.equalsIgnoreCase("com.mysql.jdbc.Driver")) {
            DBTYPE = MYSQL;
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("useSSL","false");
            properties.setProperty("verifyServerCertificate","false");
        }

        /*初始化oracle属性*/
        if (driver.equalsIgnoreCase("oracle.jdbc.driver.OracleDriver")) {
            DBTYPE = ORACLE;
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            OWNER=properties.get("OWNER").toString();
        }


        /*
        SQL默认date的值为0000-00-00，
        但 java.sql.Date 将其视为 不合法的值 格式不正确，
        然后读取的时候会报错，需要加上该属性，将date默认值转换为null
        参考 https://blog.csdn.net/ja_II_ck/article/details/3905120
        */
        properties.setProperty("zeroDateTimeBehavior","convertToNull");

        logger.info("mysqlUrl: " + driverUrl);
        logger.info("esUrl: "+ ESUrl);
    }

    /**
     * 导入数据
     */
    public static void doInput(){
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
            try {
                if(DBTYPE.equals(MYSQL))
                    getMysqlAllDatabaseStructure();
                if (DBTYPE.equals(ORACLE))
                    getOracleAllDatabaseStructure();
            } catch (IOException e) {
                logger.error("get data structure error!\n",e);
            }


            /*只生成数据字典，不导入数据*/
            if ("true".equals(justDictionary))
                return;

            /*获取所有数据*/
            getAllData();

            /*打印获取的数据总量情况*/
            logger.info("data is all in memory!");
            logger.info("========================");
            logger.info("dbNumber: "+ DatabaseNodeListInfo.dbNumber);
            logger.info("tbNumber: "+ DatabaseNodeListInfo.tbNumber);
            logger.info("rowNumber: "+ DatabaseNodeListInfo.rowNumber);
            logger.info("========================");

            /*删除与导入数据索引名相同的索引*/
            esDeleteAll();

            /*开始导入数据至es中*/
            new ESBulkData(ESUrl, DatabaseNodeListInfo.databaseNodeList).inputData();

        }
        catch(SQLException e)
        {
            logger.error("database error!",e);
        }
    }

    /**
     * 获取mysql所有库表结构，保存至databaseNodeList,并生成数据字典，输出至文件中
     * @return
     * @throws SQLException
     */
    public static void getMysqlAllDatabaseStructure() throws SQLException, IOException {
        File file = new File(dataDictionaryPath);

        //2：准备输出流
        //        Writer out = new FileWriter(file);/*这个是默认字符集，也就是*/
        Writer out = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(file), "UTF-8"
                ));

        /*连接Mysql相关变量*/
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        String sql = "select * from ";

        /*查询所有库、表、字段*/
        /*mysql*/
        con= DriverManager.getConnection(driverUrl + "information_schema", properties);

        logger.info("Connect mysql Successfull.");

        st=con.createStatement();

//            st.setString(1,"tb_person_time");
//            rs  = st.executeQuery();
        rs = st.executeQuery(sql+"columns");


        /*获取所有库、表、列名开始*/
        DatabaseNodeListInfo.databaseNodeList = new ArrayList<DatabaseNode>();

        DatabaseNode lastDB = null;
        TableNode lastTable = null;
        while(rs.next()){
            String colStr =null;
            String tbStr =null;
            String dbStr =null;
            String dataType = null;
            String colComment = null;
            if (DBTYPE.equals(MYSQL)) {
                colStr = rs.getString("COLUMN_NAME");
                tbStr = rs.getString("TABLE_NAME");
                dbStr = rs.getString("TABLE_SCHEMA");
                dataType = rs.getString("DATA_TYPE");
                colComment = rs.getString("COLUMN_COMMENT");
            }
            if (DBTYPE.equals(ORACLE)) {
                colStr = rs.getString("COLUMN_NAME");
                tbStr = rs.getString("TABLE_NAME");
                dbStr = rs.getString("OWNER");
                dataType = rs.getString("DATA_TYPE");
                colComment = rs.getString("COMMENTS");
                colComment = "";
            }

            logger.debug(dbStr+"."+tbStr+"."+colComment);
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
                    /*dbName.tbName*/
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
                     /*dbName.tbName*/
                    if (dbStr.equals(skipTB[i].split("\\.")[0]) && tbStr.equals(skipTB[i].split("\\.")[1])) {
                        skip = true;
                        break;
                    }
                }
            }

            if(skip)continue;
            /*生成数据字典：输出字段-字段comment映射表至文件中*/
            out.write(dbStr+"."+tbStr+"."+colStr+"="+colComment+"\n");

            /*保存DB相关数据*/
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
            if(lastTable==null || !tbStr.equals(lastTable.getTableName())){
                lastTable = new TableNode(tbStr);
                lastTable.getColumns().add(colStr);
                lastTable.getDataType().add(dataType);;
                lastDB.getTableNodeList().add(lastTable);
            }else{
                lastTable.getColumns().add(colStr);
                lastTable.getDataType().add(dataType);
            }
        }

        /*关闭文件输出流*/
        out.close();
        /*获取所有库、表、列名结束*/
        rs.close();
        st.close();
        con.close();
    }

    /**
     * 获取oracle所有库表结构，保存至databaseNodeList,并生成数据字典，输出至文件中
     * @return
     * @throws SQLException
     */
    public static void getOracleAllDatabaseStructure() throws SQLException, IOException {
        File file = new File(dataDictionaryPath);

        //2：准备输出流
//        Writer out = new FileWriter(file);/*这个是默认字符集，也就是*/
        Writer out = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(file), "UTF-8"
                ));

        /*连接Mysql相关变量*/
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        /*查询所有库、表、字段*/
        /*mysql*/
        con= DriverManager.getConnection(driverUrl, properties);
        logger.info("Connect oracle Successfull.");
        st=con.createStatement();
        /*获取库名、表名*/
//        String sql = "SELECT TABLE_NAME, TABLESPACE_NAME FROM all_tables WHERE OWNER='"+user.toUpperCase()+"'";/*在受限的oracle数据库中，all_tables表不能用*/
        String sql = "SELECT TABLE_NAME, OWNER FROM all_tables WHERE OWNER='"+OWNER.toUpperCase()+"'";
        logger.debug("[sql: "+sql+" ]");
        rs = st.executeQuery(sql);

        /*获取所有库、表、列名开始*/
        DatabaseNodeListInfo.databaseNodeList = new ArrayList<DatabaseNode>();
        List<DatabaseNode> dbList = DatabaseNodeListInfo.databaseNodeList;

        DatabaseNode lastDB = null;
        TableNode lastTable = null;



        while (rs.next()){
            String tbStr = rs.getString("TABLE_NAME");
            String dbStr = rs.getString("OWNER");

            logger.debug("[dbName= "+dbStr+", tbName= "+tbStr);
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
                    /*dbName.tbName*/
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
                     /*dbName.tbName*/
                    if (dbStr.equals(skipTB[i].split("\\.")[0]) && tbStr.equals(skipTB[i].split("\\.")[1])) {
                        skip = true;
                        break;
                    }
                }
            }

            if(skip)continue;

            /*判断该库是否存在*/
            if (dbList.size()!=0 && dbStr.equalsIgnoreCase(dbList.get(dbList.size()-1).getDbName())){
                List<TableNode> tbList = new ArrayList<TableNode>();
                TableNode tableNode = new TableNode(tbStr);
                dbList.get(dbList.size()-1).getTableNodeList().add(tableNode);
            }else{/*不存在则新建一个库节点，并且新建表节点*/
                List<TableNode> tbList = new ArrayList<TableNode>();
                TableNode tableNode = new TableNode(tbStr);
                tbList.add(tableNode);
                DatabaseNode dbNode = new DatabaseNode(dbStr,tbList);
                dbList.add(dbNode);
            }
        }

        /*获取表名、字段名*/
        /*只能假设，同一个owner（用户）下没有重名的表了，这里有风险*/
        String sql1 = "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from all_tab_columns WHERE OWNER='"+OWNER.toUpperCase()+"'";
        logger.debug("[sql: "+sql1+" ]");
        rs = st.executeQuery(sql1);


        while (rs.next()){
            String colStr = rs.getString("COLUMN_NAME");
            String tbStr = rs.getString("TABLE_NAME");
            String dataType = rs.getString("DATA_TYPE");
            logger.debug("[tbName="+tbStr+",colName="+colStr+",dataType="+dataType+"]");
            for (int i = 0; i < dbList.size(); ++i){
                List<TableNode> tbList = dbList.get(i).getTableNodeList();
                for (int j = 0; j < tbList.size(); ++j){
                    if (tbList.get(j).getTableName().equalsIgnoreCase(tbStr)){
                        TableNode tb = tbList.get(j);
                        tb.getColumns().add(colStr);
                        tb.getDataType().add(dataType);
                        tb.getCloumnComment().add("无");/*预先插入空值*/
                    }
                }
            }
        }

        /*获取comment备注*/
        String sql3 = "SELECT TABLE_NAME,COLUMN_NAME,COMMENTS from all_col_comments WHERE OWNER='"+OWNER.toUpperCase()+"'";
        logger.debug("[sql: "+sql3+"]");
        rs = st.executeQuery(sql3);
        while (rs.next()){
            String colStr = rs.getString("COLUMN_NAME");
            String tbStr = rs.getString("TABLE_NAME");
            String colComment = rs.getString("COMMENTS");
            logger.debug("[tbName="+tbStr+",colStr="+colStr+",colComment="+colComment+"]");
            for (int i = 0; i < dbList.size(); ++i){
                String dbStr = null;
                /*数据字典输出的db字符串*/
                if (indexDB!="")
                    dbStr = dbList.get(i).getDbName();
                else
                    dbStr = indexDB;
                List<TableNode> tbList = dbList.get(i).getTableNodeList();
                for (int j = 0; j < tbList.size(); ++j){
                    if (tbList.get(j).getTableName().equalsIgnoreCase(tbStr)){/*匹配上表名*/
                        List<String> cList = tbList.get(j).getColumns();
                        for (int k = 0; k < cList.size(); ++k){
                            if(cList.get(k).equalsIgnoreCase(colStr)){/*该表中匹配上字段就行了*/
                                tbList.get(j).getCloumnComment().add(k,colComment);
                                /*生成数据字典：输出字段-字段comment映射表至文件中*/
                                out.write(dbStr+"."+tbStr+"."+colStr+"="+colComment+"\n");
                            }
                        }
                    }
                }
            }
        }


        /*关闭文件输出流*/
        out.close();
        /*获取所有库、表、列名结束*/
        rs.close();
        st.close();
        con.close();
    }


    /**
     * 遍历表结构，获取所有数据，保存至databaseNodeList
     * @throws SQLException
     */
    public static void getAllData() throws SQLException {
        /*连接Mysql相关变量*/
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        String sql = "select * from ";
        if(DatabaseNodeListInfo.databaseNodeList==null || DatabaseNodeListInfo.databaseNodeList.size()<=0){
            logger.error("database structure is null!");
            return;
        }

        Iterator<DatabaseNode> databaseNodeIt = DatabaseNodeListInfo.databaseNodeList.iterator();
        while(databaseNodeIt.hasNext()){
            DatabaseNodeListInfo.dbNumber++;
            DatabaseNode databaseNode = databaseNodeIt.next();
                /*获取数据库连接*/
            if(DBTYPE.equalsIgnoreCase(MYSQL))
                con = DriverManager.getConnection(driverUrl +databaseNode.getDbName(),properties);
            if (DBTYPE.equalsIgnoreCase(ORACLE))
                con = DriverManager.getConnection(driverUrl,properties);
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
            while(tableNodeIterator.hasNext()){
                DatabaseNodeListInfo.tbNumber++;
                TableNode tableNode = tableNodeIterator.next();
                    /*sql查询该表所有数据*/
                st=con.createStatement();
                if(DBTYPE.equalsIgnoreCase(MYSQL))
                    rs = st.executeQuery(sql+tableNode.getTableName());
                if (DBTYPE.equalsIgnoreCase(ORACLE)) {
//                    rs = st.executeQuery(sql + "\"" + tableNode.getTableName() + "\"");
                    String sql1 = "select ";
                    for (int i = 0; i < tableNode.getColumns().size(); ++i){
                        sql1+= " \""+tableNode.getColumns().get(i)+"\",";
                    }
                    sql1 = sql1.substring(0,sql1.length()-1);/*去掉最后一个逗号*/
                    sql1+=" from \""+tableNode.getTableName()+"\"";
                    logger.debug("[sql: "+sql1+"]");
                    rs = st.executeQuery(sql1);
                }
                while(rs.next()){
                    /*所有数据+1*/
                    DatabaseNodeListInfo.rowNumber++;
                    /*该库数据+1*/
                    databaseNode.setRowNumber(databaseNode.getRowNumber()+1);

//                    ResultSetMetaData md = rs.getMetaData();
//                    int columnCount = md.getColumnCount();
//                    ArrayList<String> row = new ArrayList<String>();
//                    for(int i = 1; i <= columnCount; ++i) {
//                        row.add(rs.getString(i));
//                    }
                    ArrayList<String> row = new ArrayList<String>();
                    List cols = tableNode.getColumns();
                    for (int i = 0; i < cols.size(); ++i){
                        row.add(rs.getString(cols.get(i).toString()));
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
     * 删除已存在的同名索引，与navicat导入数据策略一致，先删再导
     */
    public static void esDeleteAll(){
        logger.info("delete already exist and conflict index ...");

            String url = "";
            if(DatabaseNodeListInfo.databaseNodeList==null || DatabaseNodeListInfo.databaseNodeList.size()<=0){
                return;
            }
            Iterator<DatabaseNode> databaseNodeIt = DatabaseNodeListInfo.databaseNodeList.iterator();
            while(databaseNodeIt.hasNext()) {
                DatabaseNode databaseNode = databaseNodeIt.next();
                Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
                while (tableNodeIterator.hasNext()) {
                    TableNode tableNode = tableNodeIterator.next();

                    /*逐个删除*/
                    url= Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName());
                    try {
                        new MyURLConnection().request(ESUrl+url,"DELETE","");
                        logger.info("delete success: "+url);
                    } catch (MalformedURLException e) {
                        logger.error("delete index error: "+url,e);
                    } catch (IOException e) {
                        logger.error("delete index error",e);
                    }
                }
            }
        logger.info("delete finished!");
    }

}
