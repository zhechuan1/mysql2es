package com.justplay1994.github.mysql2es.temp;

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
import java.util.Map;

/**
 * Created by JustPlay1994 on 2018/4/2.
 * https://github.com/JustPlay1994/daily-log-manager
 */

public class ESInsertDataThread implements Runnable{
    String dbName;
    String tbName;
    String id;
    Map<String,Object> row;
    String ESUrl;
    static Logger logger = LoggerFactory.getLogger(ESCreateMappingThread.class);


    ESInsertDataThread(String ESUrl,String dbName, String tbName, String id, Map<String,Object> row){
        this.dbName = dbName;
        this.tbName = tbName;
        this.id = id;
        this.row = row;
        this.ESUrl = ESUrl;
    }

    public void run() {
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
}
