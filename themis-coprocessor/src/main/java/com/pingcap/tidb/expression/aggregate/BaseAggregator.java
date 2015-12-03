package com.pingcap.tidb.expression.aggregate;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by shenli on 15-12-3.
 */

public abstract class BaseAggregator implements Aggregator {

    public BaseAggregator() {
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getSize() {
        return SizedUtil.OBJECT_SIZE;
    }

    ImmutableBytesWritable evalClientAggs(Aggregator clientAgg) {
        CountAggregator ca = (CountAggregator)clientAgg;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ca.evaluate(null, ptr);
        return ptr;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return null;
    }

}

