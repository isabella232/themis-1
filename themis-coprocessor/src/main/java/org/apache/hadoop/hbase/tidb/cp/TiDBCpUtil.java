package org.apache.hadoop.hbase.tidb.cp;

/**
 * Created by shenli on 15-11-25.
 */
public class TiDBCpUtil {
    /*
    GetRowByHandle(int64 handle) List<Cell>
     */

    public static String getRowKeyFromHbaseRowKey(String key) {
        // Get tidb row key from hbase row key.
        return key;
    }
}
