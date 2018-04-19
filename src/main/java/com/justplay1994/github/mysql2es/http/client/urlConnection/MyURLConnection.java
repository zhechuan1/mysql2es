package com.justplay1994.github.mysql2es.http.client.urlConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

public class MyURLConnection {
    private static final Logger logger = LoggerFactory.getLogger(MyURLConnection.class);
    public MyURLConnection(){

    }

    /**
     * 单线程 http客户端
     * @param _url
     * @param type
     * @param body
     * @return
     */
    public static String request(String _url, String type, String body){
        try {

            URL url = new URL(_url);


            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod(type);


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
            logger.debug(builder.toString());

            httpURLConnection.disconnect();/*关闭连接*/
            return builder.toString();
        } catch (MalformedURLException e) {
            logger.error("【BulkDataError】", e);
        } catch (ProtocolException e) {
            logger.error("【BulkDataError】", e);
        } catch (IOException e) {
            logger.error("【BulkDataError】", e);
        }finally {

        }
        return null;
    }
}
