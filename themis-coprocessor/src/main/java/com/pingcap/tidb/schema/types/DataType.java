package com.pingcap.tidb.schema.types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by shenli on 15-12-3.
 */
public abstract class DataType<T> implements Comparable<T> {

    private final int sqlType;
    private final TDataCodec codec;

    protected DataType(int sqlType, TDataCodec codec) {
        this.sqlType = sqlType;
        this.codec = codec;
    }

    public static interface TDataCodec {

        public long decodeLong(ImmutableBytesWritable ptr);

        public long decodeLong(byte[] b, int o);

        public int encodeLong(int v, ImmutableBytesWritable ptr);

        public int encodeLong(int v, byte[] b, int o);

    }

    public static abstract class BaseCodec implements TDataCodec {

        public long decodeLong(ImmutableBytesWritable ptr) {
            return decodeLong(ptr.get(), ptr.getOffset());
        }

        public int encodeLong(int v, ImmutableBytesWritable ptr) {
            return encodeLong(v, ptr.get(), ptr.getOffset());
        }

        public int encodeLong(int v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }
    }

}
