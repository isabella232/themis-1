package com.pingcap.tidb.expression;

import java.util.List;

import com.pingcap.tidb.schema.TRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import com.pingcap.tidb.schema.DataItem;

/**
 * Created by shenli on 15-12-2.
 */
public interface Expression extends DataItem {

    boolean evaluate(TRow row, ImmutableBytesWritable ptr);

}
