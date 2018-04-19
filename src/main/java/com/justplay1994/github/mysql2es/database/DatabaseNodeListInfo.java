/**
 *
 * Created by HZZ on 2018/4/19.
 *
 */
package com.justplay1994.github.mysql2es.database;

import com.justplay1994.github.mysql2es.Mysql2es;

import java.util.List;

/**
 * Created by HZZ on 2018/4/19.
 */
public class DatabaseNodeListInfo {
    public static List<DatabaseNode> databaseNodeList;/*所有数据*/
    public static int dbNumber=0;/*数据库总数量*/
    public static int tbNumber = 0;/*表总数量*/
    public static long rowNumber=0;/*总数据量*/
    public static int retryTimes = 0;/*重试次数*/
    public static long retryRowNumber = 0;/*重试时，数据总量*/

}
