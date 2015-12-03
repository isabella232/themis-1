package com.pingcap.tidb.expression.aggregate;

import com.pingcap.tidb.schema.TRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by shenli on 15-12-3.
 */
public class CountAggregator extends BaseAggregator {

    private long count = 0;
    private byte[] buffer = null;

    public CountAggregator() {
    }

    public CountAggregator(LongSumAggregator clientAgg) {
        this();
        count = clientAgg.getSum();
    }

    @Override
    public void aggregate(TRow tuple, ImmutableBytesWritable ptr) {
        count++;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean evaluate(TRow row, ImmutableBytesWritable ptr) {
        if (buffer == null) {
            buffer = new byte[TLong.ByteSize];
        }
        //getDataType().getCodec().encodeLong(count, buffer, 0);
        ptr.set(buffer);
        return true;
    }


    @Override
    public void reset() {
        count = 0;
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "COUNT [count=" + count + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE
                + getDataType().getByteSize();
    }
}
