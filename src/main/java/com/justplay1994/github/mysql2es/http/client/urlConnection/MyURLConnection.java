package com.justplay1994.github.mysql2es.http.client.urlConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyURLConnection {
    private static final Logger logger = LoggerFactory.getLogger(MyURLConnection.class);
    String url;
    String type;
    String body;
    String result;
    int timeout = 3000;
    public MyURLConnection(){

    }
    public MyURLConnection(int timeout){
        this.timeout = timeout;

    }

    /**
     * 单线程 http客户端
     * @param url
     * @param type
     * @param body
     * @return
     */
    public String request(String url, String type, String body) throws IOException {



//            URL url = new URL(url);
            this.url = url;
            this.type = type;
            this.body = body;

            URLConnection urlConnection = new URL(url).openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/

            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod(type);

        if(!"GET".equals(type)) {
        httpURLConnection.setDoInput(true);
        OutputStream outputStream = httpURLConnection.getOutputStream();
        outputStream.write(body.getBytes());
        }

            /*设置超时*/
            httpURLConnection.setConnectTimeout(timeout);

            httpURLConnection.connect();





            InputStream inputStream = httpURLConnection.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            result = builder.toString();
            logger.debug(result);

            httpURLConnection.disconnect();/*关闭连接*/
            return result;

    }


    public static void main(String[] args){

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                20,
                20,
                200,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(300)     //等待队列
        );

        for(int i = 50; i <= 255; ++i){
            executor.execute(new Runnable() {
                private int i;
                @Override
                public void run() {
                    try {
//                        logger.info("try :"+i);
                        new MyURLConnection(100).request("http://192.168.16."+i+":9200","GET","");
                        logger.info("success："+i);
                    } catch (IOException e) {
//                        logger.error("failed: "+i,e);
                    }
                }
                public Runnable test(int i){
                    this.i = i;
                    return this;
                }
            }.test(i));

        }
        while (!(executor.getActiveCount() == 0 && executor.getQueue().size()==0)){
            executor.shutdown();
        }
    }
}
