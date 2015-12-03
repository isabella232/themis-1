package com.pingcap.tidb.schema.types;

/**
 * Created by shenli on 15-12-3.
 */
public class TLong extends DataType<Long> {

    public static TLong INSTANCE = new TLong();

    private Long value;

    public TLong(Long l) {
        value = l;
    }
    public boolean compareTo(Long t) {
        return true;
    }
}
