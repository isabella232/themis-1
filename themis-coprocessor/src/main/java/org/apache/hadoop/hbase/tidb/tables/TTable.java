package org.apache.hadoop.hbase.tidb.tables;

/**
 * Created by shenli on 15-11-25.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.tidb.cp.generated.TiDBProto;

/**
 * TiDB table
 * Wrap TTable and functions
 * Created by shenli on 15-11-25.
 */
public class TTable {

    private TiDBProto.TTable table;

    private String keyPrefix;

    private static final Log LOG = LogFactory.getLog(TTable.class);

    public TTable(TiDBProto.TTable table) {
        this.table = table;
        this.keyPrefix = String.format("t%d_r", this.table.getID());
        LOG.info(this.keyPrefix);
    }

    public String FirstRowKey() {
        return "";
    }
    /*
    getRowKey
    isSameTable
     */
    public boolean IsSameTable(String rowKey) {
        return rowKey.startsWith(this.keyPrefix);
    }
}

