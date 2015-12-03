package com.pingcap.tidb.schema.types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by shenli on 15-12-3.
 */
public abstract class DataType<T> implements Comparable<T> {

    private final String sqlTypeName;
    private final int sqlType;
    private final Class clazz;
    private final byte[] clazzNameBytes;
    private final byte[] sqlTypeNameBytes;
    private final DataCodec codec;
    private final int ordinal;

    protected DataType(String sqlTypeName, int sqlType, Class clazz, DataCodec codec, int ordinal) {
        this.sqlTypeName = sqlTypeName;
        this.sqlType = sqlType;
        this.clazz = clazz;
        this.clazzNameBytes = Bytes.toBytes(clazz.getName());
        this.sqlTypeNameBytes = Bytes.toBytes(sqlTypeName);
        this.codec = codec;
        this.ordinal = ordinal;
    }

    public static interface PDataCodec {
        public long decodeLong(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public long decodeLong(byte[] b, int o, SortOrder sortOrder);

        public int decodeInt(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public int decodeInt(byte[] b, int o, SortOrder sortOrder);

        public byte decodeByte(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public byte decodeByte(byte[] b, int o, SortOrder sortOrder);

        public short decodeShort(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public short decodeShort(byte[] b, int o, SortOrder sortOrder);

        public float decodeFloat(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public float decodeFloat(byte[] b, int o, SortOrder sortOrder);

        public double decodeDouble(ImmutableBytesWritable ptr, SortOrder sortOrder);

        public double decodeDouble(byte[] b, int o, SortOrder sortOrder);

        public int encodeLong(long v, ImmutableBytesWritable ptr);

        public int encodeLong(long v, byte[] b, int o);

        public int encodeInt(int v, ImmutableBytesWritable ptr);

        public int encodeInt(int v, byte[] b, int o);

        public int encodeByte(byte v, ImmutableBytesWritable ptr);

        public int encodeByte(byte v, byte[] b, int o);

        public int encodeShort(short v, ImmutableBytesWritable ptr);

        public int encodeShort(short v, byte[] b, int o);

        public int encodeFloat(float v, ImmutableBytesWritable ptr);

        public int encodeFloat(float v, byte[] b, int o);

        public int encodeDouble(double v, ImmutableBytesWritable ptr);

        public int encodeDouble(double v, byte[] b, int o);

        public PhoenixArrayFactory getPhoenixArrayFactory();
    }

    public static abstract class BaseCodec implements PDataCodec {
        @Override
        public int decodeInt(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeInt(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public long decodeLong(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeLong(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public byte decodeByte(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeByte(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public short decodeShort(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeShort(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public float decodeFloat(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeFloat(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double decodeDouble(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            return decodeDouble(ptr.get(), ptr.getOffset(), sortOrder);
        }

        @Override
        public double decodeDouble(byte[] b, int o, SortOrder sortOrder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeInt(int v, ImmutableBytesWritable ptr) {
            return encodeInt(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeLong(long v, ImmutableBytesWritable ptr) {
            return encodeLong(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeByte(byte v, ImmutableBytesWritable ptr) {
            return encodeByte(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeShort(short v, ImmutableBytesWritable ptr) {
            return encodeShort(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeFloat(float v, ImmutableBytesWritable ptr) {
            return encodeFloat(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeDouble(double v, ImmutableBytesWritable ptr) {
            return encodeDouble(v, ptr.get(), ptr.getOffset());
        }

        @Override
        public int encodeInt(int v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeLong(long v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeByte(byte v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeShort(short v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeFloat(float v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodeDouble(double v, byte[] b, int o) {
            throw new UnsupportedOperationException();
        }
    }

}
