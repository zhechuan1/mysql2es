package com.justplay1994.github.mysql2es.database;

import java.util.ArrayList;

/**
 * Created by JustPlay1994 on 2018/4/3.
 * https://github.com/JustPlay1994/daily-log-manager
 */

public class TableNode {
    String tableName;
    ArrayList<String> columns;
    ArrayList<ArrayList<String>> rows;
    public ArrayList<String> dataType;/*字段类型*/

    public TableNode(){

    }

    public TableNode(String tableName){
        this.tableName = tableName;
        this.columns = new ArrayList<String>();
        this.rows = new ArrayList<ArrayList<String>>();
        this.dataType = new ArrayList<String>();
    }

    public TableNode(String tableName,ArrayList<String> columns, ArrayList<ArrayList<String>> rows, ArrayList<String> dataType){
        this.tableName = tableName;
        this.columns = columns;
        this.rows = rows;
        this.dataType = dataType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public ArrayList<ArrayList<String>> getRows() {
        return rows;
    }

    public void setRows(ArrayList<ArrayList<String>> rows) {
        this.rows = rows;
    }
}
