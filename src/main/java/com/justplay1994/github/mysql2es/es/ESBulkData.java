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
    String ESUrl;
    List<DatabaseNode> rows;/*批量数据*/
    public static final Logger logger = LoggerFactory.getLogger(ESBulkData.class);
    StringBuilder json = new StringBuilder();/*StringBuild更快，单线程使用*/
    ObjectMapper objectMapper = new ObjectMapper();
    static StringBuilder retryTable = new StringBuilder();

    public ESBulkData(String ESUrl, List<DatabaseNode> rows) {
        this.ESUrl = ESUrl;
        this.rows = rows;
    }



    public void inputData(){
        /*TODO 待优化，构造的请求体，执行了就删掉*/
//        新的方式：使用线程池
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                Mysql2es.maxThreadCount,
                10,
                200,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(8)     //等待队列
        );

        /*用于按块大小，切分批量数据请求*/
        int last = 0;/*上一次对块数据量整除，得到的值*/
        int now = 0;/*当前对块数据量整除，得到的值*/

        int blockRowNumber = 0;/*当前数据块的数据量大小，行数*/

        StringBuilder json = new StringBuilder();
        long jsonSize = 0;
        Iterator<DatabaseNode> databaseNodeIt;
        /*遍历数据，构造Mapping*/
        if(DatabaseNodeListInfo.retryTimes<=0) {/*如果不是重试，则创建mapping映射*/
            databaseNodeIt = rows.iterator();
            while (databaseNodeIt.hasNext()) {
                DatabaseNode databaseNode = databaseNodeIt.next();
                Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
                while (tableNodeIterator.hasNext()) {

                    TableNode tableNode = tableNodeIterator.next();
                    //每张表先创建mapping关系，创建location为地理信息点类型
                    /*TODO 遍历字段名，给每一个字段增加分词器
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
                    for(int i = 0; i < tableNode.getColumns().size(); ++i){
                        if(tableNode.getDataType().get(i).equals("varchar")) {
                            properties.put(tableNode.getColumns().get(i), textAnalyzer);
                        }
                    }
                    try {
                        String mapping =
                                " {\n" +
//                                        "\t\"settings\":{\n" +
//                                        "\t\t\"analysis\":{\n" +
//                                        "\t\t\t\"analyzer\":{\n" +
//                                        "\t\t\t\t\"ik\":{\n" +
//                                        "\t\t\t\t\t\"tokenizer\":\"ik_smart\"\n" +
//                                        "\t\t\t\t}\n" +
//                                        "\t\t\t}\n" +
//                                        "\t\t}\n" +
//                                        "\t},\n" +
                                        "    \"mappings\": {\n" +
                                        "        \"_doc\": {\n" +
                                        "            \"properties\": \n" +
                                        objectMapper.writeValueAsString(properties) +
                                        "            \n" +
                                        "        }\n" +
                                        "    }\n" +
                                        "}";
//                        createMapping(Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()), mapping);
                        executor.execute(new Thread(new MappingThread(Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName()),mapping)));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }


                }
            }
        }
        while(executor.getActiveCount()<=0){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                logger.error("sleep error!\n",e);
            }
        }
        /*遍历数据，构造请求参数*/
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

                /*遍历列名，找到经纬度字段对应下标*/
                for(int i = 0; i < tableNode.getColumns().size(); ++i){

                    if(Mysql2es.latStr.equalsIgnoreCase(tableNode.getColumns().get(i))){
                        lat=i;
                    }else if ((Mysql2es.lonStr.equalsIgnoreCase(tableNode.getColumns().get(i)))){
                        lon=i;
                    }else if("id".equalsIgnoreCase(tableNode.getColumns().get(i))) {
                        id=i;
                    }else{
                        if(lat!=-1 && lon!=-1 && id!=-1)break;
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
                    if(id!=-1) {
                        json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()) + "\", \"_type\": \"_doc\", \"_id\": \""+row.get(id)+"\"}}\n");
                    }else{
                        json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()) + "\", \"_type\": \"_doc\"}}\n");
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
                        json.delete(0,json.length());
                        blockRowNumber = 0;
                        /*如果当前线程数达到最大值，则阻塞等待*/
                        while(executor.getActiveCount()>=Mysql2es.maxThreadCount){
                            logger.debug("max Thread:"+executor.getActiveCount());
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
        while(executor.getActiveCount()!=0){
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
        logger.info("first finished!");
        logger.info("========================");
        logger.info("dbNumber: "+ DatabaseNodeListInfo.dbNumber);
        logger.info("tbNumber: "+DatabaseNodeListInfo.tbNumber);
        logger.info("rowNumber: "+DatabaseNodeListInfo.rowNumber);
        logger.info("total bulk body size(MB): "+jsonSize/1024/1024);
        try {
            Thread.sleep(1000);
            String str = new MyURLConnection().request(ESUrl + "/_search", "POST", "");
            Map map = objectMapper.readValue(str, Map.class);
            long esNumber = Integer.parseInt(((LinkedHashMap) map.get("hits")).get("total").toString());
            logger.info("es total row number: "+esNumber);
        } catch (IOException e) {
            logger.error("Search total error",e);
        } catch (InterruptedException e) {
            logger.error("sleep error");
        }

        logger.info("========================");





        checkAllData();

        /*判断是否有需要重新导入的表*/
        if(retryTable.length()>0 && Mysql2es.retryNumber>0){
            /*开始重试导入检查数据量失败的表*/
            Mysql2es.retryNumber--;/*剩余重试次数-1*/
            /*在数据信息中，添加重试次数以及重试数据总量*/
            DatabaseNodeListInfo.retryTimes++;/*重试次数+1*/
            /*初始化进度条*/
            ESBulkDataThread.nowRowNumber=0;/*初始化成功数量百分比*/
            ESBulkDataThread.nowFailedRowNumber=0;/*初始化失败数量百分比*/
            logger.info("************************");
            logger.info("retry the failed table");
            logger.info("already retry number: "+DatabaseNodeListInfo.retryTimes);
            logger.info("leaves retry number: "+Mysql2es.retryNumber);
            /*开始重试*/
            Mysql2es.reTryInput();
            logger.info("retry finished");
            logger.info("************************");
        }

    }


    /**
     * 检查数量
     */
    public void checkAllData(){
        DatabaseNodeListInfo.retryRowNumber=0;/*重置需要本次重试的数据总量*/
        long esTotalRowNumber = 0;
        logger.info("check data number ...");
        /*查询es相关索引的数据量，验证数据量是否一致*/
        boolean skip = true;
        Iterator<DatabaseNode> databaseNodeIt = rows.iterator();
        while (databaseNodeIt.hasNext()) {
            DatabaseNode databaseNode = databaseNodeIt.next();
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();

//            /*判断库的数据量与插入es中对应索引的数据量是否一致,TODO 如果es能直接查询到库的数据量（多个索引，索引通配符），可剪枝。*/
//            String result = new MyURLConnection().request(ESUrl + Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName()) + "/_search", "POST", "");
//            if(databaseNode.getRowNumber() == )

            while (tableNodeIterator.hasNext()) {

                TableNode tableNode = tableNodeIterator.next();

                String dbName = databaseNode.getDbName();
                String tbName = tableNode.getTableName();

                /*判断是否是重试*/
                if(DatabaseNodeListInfo.retryTimes > 0) {
                    /*判断本次重试，该表是否需要跳过*/
                    String[] str = retryTable.toString().split(",");
                    for (int i = 0; i < str.length; ++i) {/*默认跳过，直到找到需要重试的表*/
                        if (dbName.equals(str[i].split("\\.")[0]) && tbName.equals(str[i].split("\\.")[1])) {
                            skip = false;
                            break;
                        }
                    }
                    if (skip) continue;
                }

                /*计量本次es导入数据量*/
                esTotalRowNumber += tableNode.getRows().size();
                /*对比该表数据量与es导入的数据量是否一致*/
                checkTableDataNumber(dbName,tbName,tableNode.getRows().size());

            }
        }
        logger.info("this times es row number: "+esTotalRowNumber);
        logger.info("check data number finished !");
    }

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
    /*TODO 重试目前有3个BUG：1.进度条[v]、2.重复插入数据而不是更新、3.检查数量时，竟然有非重试表出现[v]、4.总数据相等，但却有3张表数据缺少？*/
    /*TODO 同上面第四条，es总数据量和各个索引加起来不相等、估计要看es的机制*/
}
