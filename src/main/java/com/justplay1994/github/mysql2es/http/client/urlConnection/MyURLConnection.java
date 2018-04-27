package com.justplay1994.github.mysql2es.http.client.urlConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

public class MyURLConnection {
    private static final Logger logger = LoggerFactory.getLogger(MyURLConnection.class);
    String url;
    String type;
    String body;
    String result;

    public MyURLConnection(){

    }

    /**
     * 单线程 http客户端
     * @param url
     * @param type
     * @param body
     * @return
     */
    public String request(String url, String type, String body) throws IOException {

        logger.debug("[url:"+url+" type:"+type+" body:"+body+"]");
//            URL url = new URL(url);
            this.url = url;
            this.type = type;
            this.body = body;

            URLConnection urlConnection = new URL(url).openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod(type);


            /*超时*/
//            httpURLConnection.setConnectTimeout(3000);


            httpURLConnection.connect();


            OutputStream outputStream = httpURLConnection.getOutputStream();

            outputStream.write(body.getBytes());


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
}
