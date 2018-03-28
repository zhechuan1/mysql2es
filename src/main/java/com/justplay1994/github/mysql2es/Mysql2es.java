package com.justplay1994.github.mysql2es;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * 同步所有库，除mysql系统库：information_schema、mysql、performance_schema
 * 库名对应es的索引，表名对应type，一行数据对应一个id
 */
public class Mysql2es {

    public static void main(String[] args){
        Mysql2es mysql2es = new Mysql2es();
        mysql2es.doPerHour();
//        HashMap hashMap = new HashMap();
//        hashMap.put("user_uuid","unified-authentication80c577f772ae43df93ca416f99a271ad");
//        hashMap.put("account","123");
//        hashMap.put("password","YTY2NWE0NTkyMDQyMmY5ZDQxN2U0ODY3ZWZkYzRmYjhhMDRhMWYzZmZmMWZhMDdlOTk4ZTg2ZjdmN2EyN2FlMw");
//        mysql2es.es("db_unified_authentication","tb_user_info","3",hashMap);
    }

    public void doPerHour(){
        String driver = "com.mysql.jdbc.Driver";
        String URL = "jdbc:mysql://localhost:3306/";
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "select * from ";
        String USERNAME = "root";
        String PASSWORD = "123456";
        String[] skipDB = {"information_schema","mysql","performance_schema"};


        try
        {
            Class.forName(driver);
        }
        catch(java.lang.ClassNotFoundException e)
        {
            System.out.println("Cant't load Driver");
        }
        try
        {
            /*查询所有库、表、字段*/
            con= DriverManager.getConnection(URL+"information_schema",USERNAME,PASSWORD);
            System.out.println("Connect Successfull.");

            st=con.createStatement();

//            st.setString(1,"tb_person_time");
//            rs  = st.executeQuery();
            rs = st.executeQuery(sql+"COLUMNS");

            HashMap<String,HashMap<String,ArrayList<String>>> dbs = new HashMap<String, HashMap<String, ArrayList<String>>>();

            while(rs.next()){

                String colStr = rs.getString("COLUMN_NAME");
                String tbStr = rs.getString("TABLE_NAME");
                String dbStr = rs.getString("TABLE_SCHEMA");
                if(dbs.get(dbStr)==null){
                    dbs.put(dbStr,new HashMap<String, ArrayList<String>>());
                }
                if(dbs.get(dbStr).get(tbStr)==null){
                    dbs.get(dbStr).put(tbStr,new ArrayList<String>());
                }
                dbs.get(dbStr).get(tbStr).add(colStr);
            }

            Set<String> dbSet = dbs.keySet();
            Iterator<String> dbIt = dbSet.iterator();
            while(dbIt.hasNext()){
                String dbName = dbIt.next();
                boolean skip = false;
                for(int s = 0; s < skipDB.length; ++s){
                    if(skipDB[s].equals(dbName)){
                        skip=true;
                        break;
                    }
                }
                if(skip)continue;
                con = DriverManager.getConnection(URL+dbName,USERNAME,PASSWORD);
                st = con.createStatement();
                Set<String> tbSet = dbs.get(dbName).keySet();
                Iterator<String> tbIt = tbSet.iterator();
                while(tbIt.hasNext()) {
                    String tbName = tbIt.next();
                    rs = st.executeQuery(sql +tbName);
                    /*将数据逐条存入es中*/
                    while(rs.next()){
                        HashMap<String,String> row = new HashMap<String, String>();
                        for(int i = 0; i < dbs.get(dbName).get(tbName).size();++i) {
                            String colName = dbs.get(dbName).get(tbName).get(i);
                            String value = rs.getString(colName);
//                            System.out.println("【"+dbName+"】【"+tbName+"】【"+colName+"】"+value);
                            if(value!=null)value = URLEncoder.encode(value,"UTF-8");
                            row.put(tbName+colName,value);/*es相同索引，即便不同type也不允许有同名字段，因为是扁平化存储*/
                        }
                        System.out.println("【"+dbName+"】【"+tbName+"】");
                        System.out.println(row);

                        es(dbName,tbName,rs.getString("id"),row);
                    }
                }
            }

            System.out.println("ok");
            rs.close();
            st.close();
            con.close();
        }
        catch(Exception e)
        {
            System.out.println("Connect fail:" + e.getMessage());
        }
    }

    public void es(String dbName, String tbName, String id, Map<String,String> row){
        try {
            /*es索引要求必须是小写*/
            dbName.toLowerCase();
            tbName.toLowerCase();

            URL url = new URL("http://www.justplay1994.win:10000/"+dbName+"/"+tbName+"/"+id);
            URLConnection urlConnection = url.openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection)urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type","application/json");
            httpURLConnection.setRequestMethod("POST");

            httpURLConnection.connect();

            OutputStream outputStream = httpURLConnection.getOutputStream();

            ObjectMapper objectMapper = new ObjectMapper();
            outputStream.write(objectMapper.writeValueAsString(row).getBytes());

            InputStream inputStream = httpURLConnection.getInputStream();
            System.out.println(inputStream);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
