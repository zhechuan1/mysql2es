//package com.justplay1994.github.mysql2es;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.*;
//import java.util.*;
//
///**
// * Created by JustPlay1994 on 2018/4/2.
// * https://github.com/JustPlay1994/daily-log-manager
// */
//
//
///**
// * 批量数据插入。
// * 沿用之前单条数据插入的方式，用数据桶去接，到一定的大小则开始关闭数据接收，并执行数据插入操作。
// * 或者获取所有的数据后，在起线程执行。这样对调用方来说更透明。
// */
//public class ESBulkData implements Runnable {
//    String ESUrl;
//    Map<String, Map<String, List>> rows;/*批量数据*/
//    static Logger logger = LoggerFactory.getLogger(ESCreateMappingThread.class);
//    StringBuilder json = new StringBuilder();
//
//    ESBulkData(String ESUrl,Map<String, Map<String, List>> rows) {
//        this.ESUrl = ESUrl;
//        this.rows = rows;
//        /*执行数据插入*/
//    }
//
//
//
//    public void run() {
//
//        try {
//
//             /*组bulk批量数据插入参数构造，开始*/
////            String json = new String();
//            StringBuilder json = new StringBuilder();
//            ObjectMapper objectMapper = new ObjectMapper();
//            Set dbSet = rows.keySet();
//            Iterator<String> dbIt = dbSet.iterator();
//            while (dbIt.hasNext()) {
//                String dbName = dbIt.next();
//                Set tbSet = rows.get(dbName).keySet();
//                Iterator<String> tbIt = tbSet.iterator();
//                while (tbIt.hasNext()) {
//                    String tbName = tbIt.next();
//                    for (int i = 0; i < rows.get(dbName).get(tbName).size(); ++i) {
//                        String row = objectMapper.writeValueAsString(rows.get(dbName).get(tbName).get(i));
//                        /*bulk请求*/
////                        json += "{ \"index\":{ \"_index\": \""+Mysql2es.indexName(dbName,tbName)+"\", \"_type\": \"_doc\"}}\n";
//                        json.append("{ \"index\":{ \"_index\": \"" + Mysql2es.indexName(dbName, tbName) + "\", \"_type\": \"_doc\"}}\n");
//                        /*bulk数据*/
////                        json += row+"\n";
//                        json.append(row + "\n");
////                        if (json.length() >= Mysql2es.BULKSIZE) {
////                            break;
////                        }
//                    }
//                }
//            }
//            /*rows数据转bulk参数，结束*/
//            URL url = null;
//
//            url = new URL(ESUrl + "_bulk");
//
//
//            URLConnection urlConnection = url.openConnection();
//            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;
//
//            /*输入默认为false，post需要打开*/
//            httpURLConnection.setDoInput(true);
//            httpURLConnection.setDoOutput(true);
//            httpURLConnection.setRequestProperty("Content-Type", "application/json");
//
//            httpURLConnection.setRequestMethod("POST");
//
//
////            httpURLConnection.setConnectTimeout(3000);
//
//
//            httpURLConnection.connect();
//
//
//            OutputStream outputStream = httpURLConnection.getOutputStream();
//
//            outputStream.write(json.toString().getBytes());
//
//
//            InputStream inputStream = httpURLConnection.getInputStream();
////            System.out.println(inputStream);
//
//            httpURLConnection.disconnect();
//        } catch (MalformedURLException e) {
//            logger.error("【BulkDataError】",e);
//        } catch (ProtocolException e) {
//            logger.error("【BulkDataError】", e);
//        } catch (IOException e) {
//            logger.error("【BulkDataError】", e);
//        }
//    }
//}
