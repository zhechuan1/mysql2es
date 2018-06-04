package com.justplay1994.github.mysql2es.es;//package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.justplay1994.github.mysql2es.Mysql2es;
import com.justplay1994.github.mysql2es.database.DatabaseNode;
import com.justplay1994.github.mysql2es.database.DatabaseNodeListInfo;
import com.justplay1994.github.mysql2es.database.TableNode;
import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by JustPlay1994 on 2018/4/2.
 * https://github.com/JustPlay1994/daily-log-manager
 */


/**
 * 批量数据插入。
 * 沿用之前单条数据插入的方式，用数据桶去接，到一定的大小则开始关闭数据接收，并执行数据插入操作。
 * 或者获取所有的数据后，在起线程执行。这样对调用方来说更透明。
 */
public class ESBulkData{
    private String ESUrl;
    List<DatabaseNode> rows;/*批量数据*/
    private static final Logger logger = LoggerFactory.getLogger(ESBulkData.class);
    static ObjectMapper objectMapper = new ObjectMapper();
    static StringBuilder retryTable = new StringBuilder();
    long jsonSize = 0; /*本次总共请求数据包体大小，单位：字节*/

    public ESBulkData(String ESUrl, List<DatabaseNode> rows) {
        this.ESUrl = ESUrl;
        this.rows = rows;
    }


    public void inputData(){
        /*创建es索引映射*/
        createMapping();
        /*导入数据*/
        doBulkOperate();
        /*校验表与索引的数据量是否相等*/
        checkAllData();
    }


    /**
     * 逐 表/索引 检查数量
     */
    public void checkAllData(){
        logger.info("========================");
        logger.info("check data number ...");
        try {
            Thread.sleep(500L);
        } catch (InterruptedException e) {
            logger.error("sleep error!",e);
        }
        long esTotalRowNumber = 0;/*统计本次es总共导入的数据量*/
        /*查询es相关索引的数据量，验证数据量是否一致*/
        Iterator<DatabaseNode> databaseNodeIt = rows.iterator();
        while (databaseNodeIt.hasNext()) {
            DatabaseNode databaseNode = databaseNodeIt.next();
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();

            while (tableNodeIterator.hasNext()) {

                TableNode tableNode = tableNodeIterator.next();

                String dbName = databaseNode.getDbName();
                String tbName = tableNode.getTableName();

                /*计量本次es导入数据量*/
                esTotalRowNumber += tableNode.getRows().size();
                /*对比该表数据量与es导入的数据量是否一致*/
                checkTableDataNumber(dbName,tbName,tableNode.getRows().size());

            }
        }
        logger.info("this times es row number: "+esTotalRowNumber);
        logger.info("this times data row number: "+ DatabaseNodeListInfo.rowNumber);
        logger.info("check data number finished !");
    }

    /**
     * 检查该表的数据总行数与es对应索引的总行数是否相等
     * @param dbName
     * @param tbName
     * @param tableRowsSize
     */
    public void checkTableDataNumber(String dbName,String tbName, long tableRowsSize){
        try {
            String result = new MyURLConnection().request(ESUrl + Mysql2es.indexName(dbName, tbName) + "/_search", "POST", "");
            Map map = objectMapper.readValue(result, Map.class);
            long esNumber = Integer.parseInt(((LinkedHashMap) map.get("hits")).get("total").toString());
            if (tableRowsSize != esNumber) {
                logger.error("check number is not consistent! dbName= " + dbName+
                        ", tbName= " + tbName + ", tbRowNumber= " + tableRowsSize +
                        ", esRowNumber= " + esNumber);
                retryTable.append(dbName).append(".").append(tbName).append(",");
                DatabaseNodeListInfo.retryRowNumber += tableRowsSize;
            }

        } catch (IOException e) {
            logger.error("[check number error]", e);
        }
    }

    /**
     * 创建es索引映射
     */
    public void createMapping(){
        logger.info("begin create es mapping...");
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0,
                Mysql2es.maxThreadCount,
                100,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(Mysql2es.maxThreadCount)     //等待队列
        );
        /*遍历数据，构造Mapping*/
        Iterator<DatabaseNode> databaseNodeIt = null;
            databaseNodeIt = rows.iterator();
            while (databaseNodeIt.hasNext()) {
                DatabaseNode databaseNode = databaseNodeIt.next();
                Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
                while (tableNodeIterator.hasNext()) {

                    TableNode tableNode = tableNodeIterator.next();
                    //每张表先创建mapping关系，创建location为地理信息点类型
                    /*遍历字段名，给每一个字段增加分词器
                    * 每个属性增加：
                    * {"type":"text",
                	    "analyzer": "ik_max_word",
                	    "search_analyzer": "ik_max_word"}
                    * */
                    HashMap properties = new HashMap();
                    /*地理信息字段*/
                    HashMap geo = new HashMap();
                    geo.put("type","geo_point");
                    properties.put("location",geo);
                    /*分词字段*/
                    HashMap textAnalyzer = new HashMap();
                    textAnalyzer.put("type","text");
                    textAnalyzer.put("analyzer","ik_max_word");
                    textAnalyzer.put("search_analyzer","ik_max_word");
                    /*时间字段*/
                    HashMap dateTime = new HashMap();
                    dateTime.put("type","date");
                    dateTime.put("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");

                    for(int i = 0; i < tableNode.getColumns().size(); ++i){
                        if (Mysql2es.DBTYPE.equalsIgnoreCase(Mysql2es.MYSQL)) {
                            if (tableNode.getDataType().get(i).equalsIgnoreCase("varchar")) {
                                properties.put(tableNode.getColumns().get(i), textAnalyzer);
                            } else if ("true".equalsIgnoreCase(Mysql2es.DateTime)) {
                                if (tableNode.getDataType().get(i).equalsIgnoreCase("datetime")) {
                                    properties.put(tableNode.getColumns().get(i), dateTime);
                                }
                            }
                        }
                        if (Mysql2es.DBTYPE.equalsIgnoreCase(Mysql2es.ORACLE)) {
                            if (tableNode.getDataType().get(i).equalsIgnoreCase("NVARCHAR2")) {
                                properties.put(tableNode.getColumns().get(i), textAnalyzer);
                            }
                        }
                    }
                    try {
                        String mapping =
                                " {\n" +
                                "    \"mappings\": {\n" +
                                "        \""+Mysql2es.indexType+"\": {\n" +
                                "            \"properties\": \n" +
                                objectMapper.writeValueAsString(properties) +
                                "            \n" +
                                "        }\n" +
                                "    }\n" +
                                "}";

                        String indexName = Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName());
                        executor.execute(new Thread(new MappingThread(indexName,mapping)));
                        /*如果当前线程数达到最大值，则阻塞等待*/
                        while(executor.getQueue().size()>=executor.getMaximumPoolSize()){
                            logger.debug("Thread waite ...Already maxThread. Now Thread nubmer:"+executor.getActiveCount());
//                            logger.debug("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+executor.getQueue().size()+"，已执行完别的任务数目："+executor.getCompletedTaskCount());
                            long time = 100;
                            try {
                                Thread.sleep(time);
                            } catch (InterruptedException e) {
                                logger.error("sleep error!",e);
                            }
                        }
                    } catch (JsonProcessingException e) {
                        logger.error("json error!\n",e);
                    }

                }
            }
            while(executor.getActiveCount()!=0 || executor.getQueue().size()!=0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("sleep error!\n",e);
                }
            }
            /*关闭线程池*/
            executor.shutdown();
            logger.info("Finished to create es mapping!");
        }

    /**
     * 批量导入数据
     */
    public void doBulkOperate(){
        logger.info("begin input data...");
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0,
                Mysql2es.maxThreadCount,
                100,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(Mysql2es.maxThreadCount)     //等待队列
        );

        int blockRowNumber = 0;/*当前数据块的数据量大小，行数*/

        StringBuilder json = new StringBuilder();/*单线程使用*/


        /*遍历数据，构造请求参数*/
        Iterator<DatabaseNode> databaseNodeIt;
        databaseNodeIt = rows.iterator();
        boolean skip = true;
        while(databaseNodeIt.hasNext()){/*遍历库*/
            DatabaseNode databaseNode = databaseNodeIt.next();
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
            while(tableNodeIterator.hasNext()){/*遍历表*/
                TableNode tableNode = tableNodeIterator.next();
                /*判断是否是重试*/
                if(DatabaseNodeListInfo.retryTimes > 0) {
                    /*判断本次重试，该表是否需要跳过*/
                    String[] str = retryTable.toString().split(",");
                    for (int i = 0; i < str.length; ++i) {/*默认跳过，直到找到需要重试的表为止*/
                        if (databaseNode.getDbName().equals(str[i].split("\\.")[0]) && tableNode.getTableName().equals(str[i].split("\\.")[1])) {
                            skip = false;
                            break;
                        }
                    }
                    if (skip) continue;
                }

                /*找到经纬度的下标*/
                int lat=-1;
                int lon=-1;
                /*找到id下标*/
                int id=-1;
                /*类型为dateTime的下标*/
                List<Integer> dateTime=new ArrayList<Integer>();
                /*遍历列名，找到经纬度字段对应下标*/
                for(int i = 0; i < tableNode.getColumns().size(); ++i){
                    if(Mysql2es.latStr.equalsIgnoreCase(tableNode.getColumns().get(i))){
                        lat=i;
                    }else if ((Mysql2es.lonStr.equalsIgnoreCase(tableNode.getColumns().get(i)))){
                        lon=i;
                    }else if("id".equalsIgnoreCase(tableNode.getColumns().get(i))) {
                        id=i;
                    } else{
                        if(lat!=-1 && lon!=-1 && id!=-1)break;
                    }
                }
                for (int i = 0; i < tableNode.getDataType().size(); ++i){
                    if("datetime".equalsIgnoreCase(tableNode.getDataType().get(i))){
                        dateTime.add(i);
                        continue;
                    }
                }

                /*遍历数据*/
                Iterator<ArrayList<String>> iterator = tableNode.getRows().iterator();
                while (iterator.hasNext()) {
                    blockRowNumber++;

                    Map map = new HashMap();/*数据*/
                    ArrayList<String> row = iterator.next();
                    for(int i = 0; i < row.size(); ++i){
                        /*去除空数据*/
                        if(row.get(i)!=null && !row.get(i).equals(""))
                            map.put(tableNode.getColumns().get(i),row.get(i));
                    }
                    if("true".equalsIgnoreCase(Mysql2es.DateTime)) {
                    /*处理时间字段，截取2018-05-02 16:13:00.0 的最后两位 .0*/
                        for (int index : dateTime) {
                            String str = row.get(index);
                            if(str==null||str.isEmpty())continue;
                            str = str.substring(0, str.length() - 2);
                            map.put(tableNode.getColumns().get(index), str);
                        }
                    }
                    /*根据已找到的经纬度字段，进行格式转换*/
                    if(lat!=-1 && lon!=-1){
                        if(row.get(lat)!=null && row.get(lon)!=null) {
                            HashMap location = new HashMap();
                            location.put("lat", row.get(lat));
                            location.put("lon", row.get(lon));
                            map.put("location", location);
                        }
                    }
                    /*请求head*/
                    if(id!=-1) {/*没有id字段，es自动生成uuid*/
                        json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()) + "\", \"_type\": \""+Mysql2es.indexType+"\", \"_id\": \""+row.get(id)+"\"}}\n");
                    }else{
                        json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()) + "\", \"_type\": \""+Mysql2es.indexType+"\"}}\n");
                    }

                    /*请求body*/
                    ObjectMapper objectMapper = new ObjectMapper();
                    try {
                        json.append(objectMapper.writeValueAsString(map) + "\n");
                    } catch (JsonProcessingException e) {
                        logger.error("To json error!",e);
                    }
                    /*等待请求body满一个数据块大小，便开始执行请求*/
                    if(json.length()>=Mysql2es.BULKSIZE) {
                        /*为了节约内存空间，每次请求完成，则删除请求体*/
                        executor.execute(new Thread(new ESBulkDataThread(ESUrl, json.toString(), blockRowNumber)));
                        jsonSize+=json.length();
                        json.delete(0,json.length());/*清空请求数据体*/
                        blockRowNumber = 0;
                        /*如果当前线程数达到最大值，则阻塞等待*/
                        while(executor.getQueue().size()>=executor.getMaximumPoolSize()){
                            logger.debug("Already maxThread. Now Thread nubmer:"+executor.getActiveCount());
//                            logger.debug("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+executor.getQueue().size()+"，已执行玩别的任务数目："+executor.getCompletedTaskCount());
                            long time = 200;
                            try {
                                Thread.sleep(time);
                            } catch (InterruptedException e) {
                                logger.error("sleep error!",e);
                            }
                        }
                    }
                }
            }
        }
        /*将剩余不足一个数据块的数据单独发起请求*/
        executor.execute(new Thread(new ESBulkDataThread(ESUrl, json.toString(), blockRowNumber)));

        /*阻塞等待线程结束*/
        while(executor.getActiveCount()!=0 || executor.getQueue().size()!=0){
//            logger.info("wait thread number : " + executor.getActiveCount());
            long time = 1000;
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                logger.error("sleep error!",e);
            }
        }

        /*关闭线程池*/
        executor.shutdown();
        logger.info("Input data finished!");
        logger.info("========================");
        logger.info("dbNumber: "+ DatabaseNodeListInfo.dbNumber);
        logger.info("tbNumber: "+DatabaseNodeListInfo.tbNumber);
        logger.info("rowNumber: "+DatabaseNodeListInfo.rowNumber);
        logger.info("total bulk body size(MB): "+jsonSize/1024/1024);
    }

}
