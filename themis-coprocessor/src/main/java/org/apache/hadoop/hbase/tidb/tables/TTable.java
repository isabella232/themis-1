package org.apache.hadoop.hbase.tidb.tables;

/**
 * Created by shenli on 15-11-25.
 */

import org.apache.hadoop.hbase.tidb.cp.generated.TiDBProto;

/**
 * TiDB table
 * Wrap TTable and functions
 * Created by shenli on 15-11-25.
 */
public class TTable {
    private TiDBProto.TTable table;

    public TTable(TiDBProto.TTable table) {
        this.table = table;
    }

    public String FirstRowKey() {
        return "";
    }
    /*
    getRowKey
    isSameTable
     */
}

