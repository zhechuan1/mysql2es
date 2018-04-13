package com.justplay1994.github.mysql2es.es;//package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.justplay1994.github.mysql2es.Mysql2es;
import com.justplay1994.github.mysql2es.database.DatabaseNode;
import com.justplay1994.github.mysql2es.database.TableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;

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
    int index = 0;
    ObjectMapper objectMapper = new ObjectMapper();
    public ESBulkData(String ESUrl, List<DatabaseNode> rows) {
        this.ESUrl = ESUrl;
        this.rows = rows;
        /*执行数据插入*/
    }



    public void inputData(){


        /*用于按块大小，切分批量数据请求*/
        int last = 0;/*上一次对块数据量整除，得到的值*/
        int now = 0;/*当前对块数据量整除，得到的值*/

        int blockRowNumber = 0;/*当前数据块的数据量大小，行数*/

        StringBuilder json = new StringBuilder();
        /*遍历数据，构造请求参数*/
        Iterator<DatabaseNode> databaseNodeIt = rows.iterator();
        while(databaseNodeIt.hasNext()){
            DatabaseNode databaseNode = databaseNodeIt.next();
            Iterator<TableNode> tableNodeIterator = databaseNode.getTableNodeList().iterator();
            while(tableNodeIterator.hasNext()){

                TableNode tableNode = tableNodeIterator.next();
                //每张表先创建mapping关系
//                new ESGeoMapping(Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName()),ESUrl);

                String mapping =
                        "        {\n" +
                        "            \"mappings\": {\n" +
                        "                \"_doc\": {\n" +
                        "                    \"properties\": {\n" +
                        "                        \"location\": { \"type\": \"geo_point\"  }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }";
                createMapping(Mysql2es.indexName(databaseNode.getDbName(),tableNode.getTableName()),mapping);

                /*找到经纬度的下标*/
                int lat=-1;
                int lon=-1;

                /*遍历列名*/
                for(int i = 0; i < tableNode.getColumns().size(); ++i){

                    if(Mysql2es.latStr.equals(tableNode.getColumns().get(i))){
                        lat=i;
                    }else if ((Mysql2es.lonStr.equals(tableNode.getColumns().get(i)))){
                        lon=i;
                    }else{
                        if(lat!=-1 && lon!=-1)break;
                    }
                }

                /*遍历数据*/
                Iterator<ArrayList<String>> iterator = tableNode.getRows().iterator();
                while (iterator.hasNext()) {
                    blockRowNumber++;

                    Map map = new HashMap();/*数据*/
                    ArrayList<String> row = iterator.next();
                    for(int i = 0; i < row.size(); ++i){
                        map.put(tableNode.getColumns().get(i),row.get(i));

                    }
                    /*有经纬度信息，则需要进行转换*/
                    if(lat!=-1 && lon!=-1){
                        HashMap location = new HashMap();
                        location.put("lat",row.get(lat));
                        location.put("lon",row.get(lon));
                        map.put("location",location);
                    }
                    /*请求head*/
                    json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(databaseNode.getDbName(), tableNode.getTableName()) + "\", \"_type\": \"_doc\"}}\n");
                    /*请求body*/
                    ObjectMapper objectMapper = new ObjectMapper();
                    try {
                        json.append(objectMapper.writeValueAsString(map) + "\n");
                    } catch (JsonProcessingException e) {
                        logger.error("To json error!",e);
                    }
                    now = json.length()/Mysql2es.BULKSIZE;
                    if(now >last) {

                        last = now;
                        new Thread(new ESBulkDataThread(ESUrl, json.substring(index).toString(), blockRowNumber)).start();
                        index=json.length()-1;
                        ESBulkDataThread.threadCount++;
                        while(ESBulkDataThread.threadCount>=Mysql2es.maxThreadCount){
                            logger.debug("max Thread:"+ESBulkDataThread.threadCount);
                            long time = 200;
                            try {
                                Thread.sleep(time);
                            } catch (InterruptedException e) {
                                logger.error("sleep error!",e);
                            }
                        }
                        blockRowNumber = 0;
                    }
                }

            }
        }
        /*执行剩下的数据插入动作*/
        ESBulkDataThread.threadCount++;
        new Thread(new ESBulkDataThread(ESUrl, json.substring(index).toString(), blockRowNumber)).start();
        while(ESBulkDataThread.threadCount!=0){
            logger.info("wait thread number : " + ESBulkDataThread.threadCount);
            long time = 1000;
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                logger.error("sleep error!",e);
            }
        }
        logger.info("All finished!");
    }

    public void createMapping(String indexName, String mapping){
        logger.info("creating mapping...");

        /*创建索引映射*/

        try {
            URL url;

            url = new URL(ESUrl + indexName);
            logger.debug(url.toString());
            logger.debug(mapping);


            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod("PUT");


//            httpURLConnection.setConnectTimeout(3000);


            httpURLConnection.connect();


            OutputStream outputStream = httpURLConnection.getOutputStream();

            outputStream.write(mapping.getBytes());


            InputStream inputStream = httpURLConnection.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            logger.debug(builder.toString());

            httpURLConnection.disconnect();
            logger.info("mapping finished!");
        } catch (MalformedURLException e) {
            logger.error("【MappingError】", e);
        } catch (ProtocolException e) {
            logger.error("【MappingError】", e);
        } catch (IOException e) {
            logger.error("【MappingError】", e);
        }
    }
}
