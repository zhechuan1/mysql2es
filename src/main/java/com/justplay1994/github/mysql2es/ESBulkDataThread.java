package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * Created by JustPlay1994 on 2018/4/3.
 * https://github.com/JustPlay1994/daily-log-manager
 */

public class ESBulkDataThread implements Runnable {
    public static final Logger logger = LoggerFactory.getLogger(ESBulkDataThread.class);

    static int threadCount = 0;
    String ESUrl;
    String json;
    static int nowRowNumber = 0; /*已导入数据量*/
    private int blockRowNumber = 0;/*当前数据块大小*/

    public ESBulkDataThread(String ESUrl, String json, int blockRowNumber){
        this.ESUrl = ESUrl;
        this.json = json;
        this.blockRowNumber = blockRowNumber;
    }

    public void run() {
        try {
        /*rows数据转bulk参数，结束*/
            logger.info("input begin! Thread count = " + threadCount);
            URL url = null;

            url = new URL(ESUrl + "_bulk");


            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod("POST");


//            httpURLConnection.setConnectTimeout(3000);


            httpURLConnection.connect();


            OutputStream outputStream = httpURLConnection.getOutputStream();

            outputStream.write(json.toString().getBytes());


            InputStream inputStream = httpURLConnection.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            logger.debug(builder.toString());

            /*201是成功插入，209是失败，*/
            ObjectMapper objectMapper = new ObjectMapper();
            Map map =objectMapper.readValue(builder.toString().getBytes(),Map.class);
            if("ture".equals(map.get("errors"))){
                logger.error("insert error:");
                logger.error(url.toString());
                logger.error(json);
            }

            httpURLConnection.disconnect();

        } catch (MalformedURLException e) {
            logger.error("【BulkDataError】", e);
        } catch (ProtocolException e) {
            logger.error("【BulkDataError】", e);
        } catch (IOException e) {
            logger.error("【BulkDataError】", e);
        }finally {
            changeThreadCount();/*同步操作，互斥锁*/
            logger.info("Thread input end! Thread count = " + threadCount);
            changeNowRowNumber();/*打印进度条*/

        }
    }

    synchronized public static void changeThreadCount() {
        threadCount --;
    }

    public int getBlockRowNumber() {
        return blockRowNumber;
    }

    public void setBlockRowNumber(int blockRowNumber) {
        this.blockRowNumber = blockRowNumber;
    }

    /*打印进度条*/
    synchronized public void changeNowRowNumber(){
        nowRowNumber+=blockRowNumber;
        DecimalFormat df = new DecimalFormat("0.00");
        logger.info("has finished: "+ df.format(((float)nowRowNumber/Mysql2es.rowNumber)*100)+"%");
    }
}
