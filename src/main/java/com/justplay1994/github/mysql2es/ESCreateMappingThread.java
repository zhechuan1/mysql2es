package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;

/**
 * Created by JustPlay1994 on 2018/4/2.
 * https://github.com/JustPlay1994/daily-log-manager
 */

public class ESCreateMappingThread implements Runnable{
    String dbName;
    String tbName;
    Map<String,Object> row;
    String ESUrl;

    static Logger logger = LoggerFactory.getLogger(ESCreateMappingThread.class);

    ESCreateMappingThread(String ESUrl,String dbName, String tbName,Map<String,Object> row){
        this.dbName = dbName;
        this.tbName = tbName;
        this.row = row;
        this.ESUrl = ESUrl;
    }

    public void run() {
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
