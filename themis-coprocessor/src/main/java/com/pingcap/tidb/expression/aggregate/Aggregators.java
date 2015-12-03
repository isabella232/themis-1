package com.pingcap.tidb.expression.aggregate;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by shenli on 15-12-2.
 */

abstract public class Aggregators {

    protected final Aggregator[] aggregators;

    public Aggregators() {
    }
}
