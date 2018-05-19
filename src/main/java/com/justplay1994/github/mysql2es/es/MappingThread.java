package com.justplay1994.github.mysql2es.es;

import com.justplay1994.github.mysql2es.Mysql2es;
import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ProtocolException;

/**
 * 创建mapping的线程实例
 */
public class MappingThread implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MappingThread.class);

    String ESUrl = Mysql2es.ESUrl;

    String indexName;
    String mapping;

    public MappingThread(String indexName, String mapping){
        this.indexName = indexName;
        this.mapping = mapping;
    }

    public void createMapping(){
        logger.info("creating mapping...");

        /*创建索引映射*/

        try {
            new MyURLConnection().request(ESUrl + indexName,"PUT",mapping);
            logger.info("mapping finished! indexName: "+ indexName);
        } catch (MalformedURLException e) {
            logger.error("【MappingError】", e);
            logger.error("url: "+ESUrl+indexName+"\n "+ mapping);
        } catch (ProtocolException e) {
            logger.error("【MappingError】", e);
            logger.error("url: "+ESUrl+indexName+"\n "+ mapping);
        } catch (IOException e) {
            logger.error("【MappingError】", e);
            logger.error("url: "+ESUrl+indexName+"\n "+ mapping);
        }
    }

    public void run() {
        createMapping();
    }
}
