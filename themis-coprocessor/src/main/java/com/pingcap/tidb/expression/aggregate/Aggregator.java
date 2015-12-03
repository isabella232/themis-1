package com.pingcap.tidb.expression.aggregate;

import com.pingcap.tidb.schema.TRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by shenli on 15-12-2.
 */
public interface Aggregator extends Expression {

    public void aggregate(TRow row, ImmutableBytesWritable ptr);

    public int getSize();
}
