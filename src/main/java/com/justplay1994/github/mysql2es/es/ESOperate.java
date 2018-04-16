package com.justplay1994.github.mysql2es.es;

import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;

public class ESOperate {
    public static void main(String[] args){
        ESOperate esOperate = new ESOperate();
        System.out.println(esOperate.query_all());
    }

    /**
     * 查询所有数据
     */
    public String query_all(){
        String body = "{\n" +
                "\t\"query\":{\n" +
                "\t\t\"match_all\":{}\n" +
                "\t}\n" +
                "}";
        return MyURLConnection.request("http://192.168.16.54:9200/_search","POST",body);
    }

    public String query_geo(){
        String body ="";
        return MyURLConnection.request("http://192.168.16.54:9200/_search","POST",body);
    }
}
