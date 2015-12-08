package com.pingcap.tidb.expression.aggregate;

import com.pingcap.tidb.schema.TRow;
import com.pingcap.tidb.schema.types.TLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by shenli on 15-12-3.
 */
public class CountAggregator extends BaseAggregator {

    private long count = 0;
    private byte[] buffer = null;

    private static final Log LOG = LogFactory.getLog(CountAggregator.class);

    public CountAggregator() {}

    @Override
    public void aggregate(TRow row, ImmutableBytesWritable ptr) {
        count++;
        LOG.info("Count: " + count);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    public boolean evaluate(TRow row, ImmutableBytesWritable ptr) {
        LOG.info("Get Count: " + this.count);
        if (buffer == null) {
            buffer = new byte[TLong.ByteSize];
            Bytes.putLong(buffer, 0, this.count);
        }
        ptr.set(buffer);
        return true;
    }

    public void reset() {
        count = 0;
        buffer = null;
    }

    @Override
    public String toString() {
        return "COUNT [count=" + count + "]";
    }

    @Override
    public int getSize() {
        return 0;
    }
}
