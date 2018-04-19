package com.justplay1994.github.mysql2es.es;

import com.justplay1994.github.mysql2es.http.client.urlConnection.MyURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ESOperate {
    private final static Logger logger = LoggerFactory.getLogger(ESOperate.class);

    public static void main(String[] args){
        ESOperate esOperate = new ESOperate();
        System.out.println(esOperate.query_all());
        System.out.println(esOperate.query_geo());
        System.out.println(esOperate.query_properties_exact());
        System.out.println(esOperate.query_properties_fuzzy());
        System.out.println(esOperate.query_page());
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
        try {
            return new MyURLConnection().request("http://192.168.16.54:9200/_search","POST",body);
        } catch (IOException e) {
            logger.error("[query_all error]",e);
        }
        return null;
    }

    /**
     * 地理信息多边形搜索
     * @return
     */
    public String query_geo(){
        String body ="{\n" +
                "    \"query\": {\n" +
                "        \"bool\" : {\n" +
                "            \"must\" : {\n" +
                "                \"match_all\" : {}\n" +
                "            },\n" +
                "            \"filter\" : {\n" +
                "                \"geo_polygon\" : {\n" +
                "                    \"location\" : {\n" +
                "                        \"points\" : [\n" +
                "                            [114,22],\n" +
                "            [115,22],\n" +
                "            [115,23],\n" +
                "            [114,23.1],\n" +
                "            [115,21],\n" +
                "            [114,22]\n" +
                "                        ]\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        try {
            return new MyURLConnection().request("http://192.168.16.54:9200/_search","POST",body);
        } catch (IOException e) {
            logger.error("[query_geo error]",e);
        }
        return null;
    }

    /**
     * 精确查询某一个属性
     * 实例：查询name
     * @return
     */
    public String query_properties_exact(){
        String body = "{\n" +
                "    \"query\" : {\n" +
                "        \"bool\":{\n" +
                "        \t\"must\":{\n" +
                "        \t\t\"match_phrase\" : { \n" +
                "                    \"name\": \"红辣椒川菜火锅城\"\n" +
                "                }\n" +
                "        \t}\n" +
                "        }\n" +
                "    }\n" +
                "}";
        try {
            return new MyURLConnection().request("http://192.168.16.54:9200/_search","POST",body);
        } catch (IOException e) {
            logger.error("[query_properties_exact error]",e);
        }
        return null;
    }

    /**
     * 模糊查询某一个属性
     * 实例：查询name
     * @return
     */
    public String query_properties_fuzzy(){
        String body = "{\n" +
                "    \"query\" : {\n" +
                "        \"bool\":{\n" +
                "        \t\"must\":{\n" +
                "        \t\t\"match\" : { \n" +
                "                    \"name\": \"红辣椒川菜火锅城\"\n" +
                "                }\n" +
                "        \t}\n" +
                "        }\n" +
                "    }\n" +
                "}";
        try {
            return new MyURLConnection().request("http://192.168.16.54:9200/_search","POST",body);
        } catch (IOException e) {
            logger.error("[query_properties_fuzzy error]",e);
        }
        return null;
    }

    /**
     * 分页查询
     * 实例：查询name
     * size:返回查询数量
     * from:丢掉结果数量
     * 建议：查询结果不要超过1000，因为分页其实是查询出来后，需要整体进行排序，时间复杂度为指数。
     * @return
     */
    public String query_page(){
        String body = "{\n" +
                "\t\"query\":{\n" +
                "\t\t\"match_all\":{}\n" +
                "\t}\n" +
                "}";
        try {
            return new MyURLConnection().request("http://192.168.16.54:9200/_search?size=1&from=1","POST",body);
        } catch (IOException e) {
            logger.error("[query_page error]",e);
        }
        return null;
    }


}
