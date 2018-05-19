package com.justplay1994.github.mysql2es.database;

import java.util.ArrayList;


/**
 * 表节点，存放该表所有数据、字段名、字段类型、字段描述
 */
public class TableNode {
    String tableName;       /*表名*/
    ArrayList<String> columns;  /*字段名*/
    ArrayList<ArrayList<String>> rows;  /*数据列表*/
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
