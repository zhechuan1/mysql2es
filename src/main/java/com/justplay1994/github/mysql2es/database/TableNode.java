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
    private ArrayList<String> dataType;/*字段类型*/
    ArrayList<String> cloumnComment; /*字段描述*/

    public TableNode(){

    }

    public ArrayList<String> getDataType() {
        return dataType;
    }

    public void setDataType(ArrayList<String> dataType) {
        this.dataType = dataType;
    }

    public ArrayList<String> getCloumnComment() {
        return cloumnComment;
    }

    public void setCloumnComment(ArrayList<String> cloumnComment) {
        this.cloumnComment = cloumnComment;
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
