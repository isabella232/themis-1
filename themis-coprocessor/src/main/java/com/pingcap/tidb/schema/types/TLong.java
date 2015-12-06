package com.pingcap.tidb.schema.types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by shenli on 15-12-3.
 */
public class TLong extends DataType<TLong> {

    public static TLong INSTANCE = new TLong();

    private long value;

    public static int ByteSize = 4;

    public TLong(Long l) {
        super(1, new TLongCodec());
        value = l;
    }

    public int compareTo(TLong t) {
        if (this.value < t.value) {
            return -1;
        } else if (this.value > t.value) {
            return 1;
        }
        return 0;
    }

    public static class TLongCodec extends BaseCodec {

        public long decodeLong(byte[] b, int o) {
            return 1;
        }

        public int encodeLong(long v, ImmutableBytesWritable ptr) {
            return 1;
        }

        public int encodeLong(long v, byte[] b, int o) {
            return 1;
        }

    }
}
