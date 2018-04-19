package com.justplay1994.github.mysql2es.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.justplay1994.github.mysql2es.database.DatabaseNodeListInfo;
import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * Created by JustPlay1994 on 2018/4/3.
 * https://github.com/JustPlay1994/daily-log-manager
 */

public class ESBulkDataThread implements Runnable {
    public static final Logger logger = LoggerFactory.getLogger(ESBulkDataThread.class);

//    static int threadCount = 0;
    String ESUrl;

    static int nowRowNumber = 0; /*已导入数据量*/
    static int nowFailedRowNumber = 0; /*已导入失败的数据量*/
    private int blockRowNumber = 0;/*当前数据块大小*/

    /**
     * 请求相关参数
     */
    String url;
    String type;
    String json;
    String result;/*请求返回数据包*/

    public ESBulkDataThread(String ESUrl, String json, int blockRowNumber){
        this.ESUrl = ESUrl;
        this.json = json;
        this.blockRowNumber = blockRowNumber;
    }

    public void run() {
        try {
            /*开始导入数据，当前工作线程数量打印*/
//            logger.info("input begin! Thread count = " + threadCount);

            url = ESUrl + "_bulk";
            type = "POST";/*必须大写*/
            result = new MyURLConnection().request(url,type,json);

            logger.debug(getRequestFullData());

            /*201是成功插入，209是失败，*/
            ObjectMapper objectMapper = new ObjectMapper();
            Map map =objectMapper.readValue(result.getBytes(),Map.class);
            if("ture".equals(map.get("errors"))){
                logger.error("insert error:");
                logger.error(getRequestFullData());
                addNowFailedRowNumber(blockRowNumber);

            }else {
                addNowRowNumber(blockRowNumber);
//                nowRowNumber+=blockRowNumber;
            }

        } catch (MalformedURLException e) {
            addNowFailedRowNumber(blockRowNumber);
            logger.error("【BulkDataError1】", e);
            logger.error(getRequestFullData());
        } catch (ProtocolException e) {
            addNowFailedRowNumber(blockRowNumber);
            logger.error("【BulkDataError2】", e);
            logger.error(getRequestFullData());
        } catch (IOException e) {
            addNowFailedRowNumber(blockRowNumber);
            logger.error("【BulkDataError3】", e);
            logger.error(getRequestFullData());
        }finally {
//            changeThreadCount();/*同步操作，互斥锁*/
//            logger.info("Thread input end! Thread count = " + threadCount);
            changeNowRowNumber();/*打印进度条*/
        }
    }

//    synchronized public static void changeThreadCount() {
//        threadCount --;
//    }

    /*打印进度条*/
    synchronized public void changeNowRowNumber(){

        DecimalFormat df = new DecimalFormat("0.00");
        if(DatabaseNodeListInfo.retryTimes>0) {/*如果是重试，则打印重试总数据量的进度条*/
            logger.info("has finished: " + df.format(((float) nowRowNumber / DatabaseNodeListInfo.retryRowNumber) * 100) + "%");
            logger.info("has error: " + df.format(((float) nowFailedRowNumber / DatabaseNodeListInfo.retryRowNumber) * 100) + "%");
        }else {/*非重试，则打印总数据量的进度条*/
            logger.info("has finished: " + df.format(((float) nowRowNumber / DatabaseNodeListInfo.rowNumber) * 100) + "%");
            logger.info("has error: " + df.format(((float) nowFailedRowNumber / DatabaseNodeListInfo.rowNumber) * 100) + "%");
        }
    }

    /*获取完整请求信息*/
    public String getRequestFullData(){
        return "[request] url:"+url+",type:"+type+",body:"+json+"" +
                "\n[result]: "+result;
    }

    synchronized void addNowRowNumber(long addNumber){
        nowRowNumber+=addNumber;
    }

    synchronized void addNowFailedRowNumber(long addNumber){
        nowFailedRowNumber+=addNumber;
    }
}
