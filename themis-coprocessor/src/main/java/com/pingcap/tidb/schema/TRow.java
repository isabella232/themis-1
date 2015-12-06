package com.pingcap.tidb.schema;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.util.List;

/**
 * Created by shenli on 15-12-3.
 */
public class TRow {

    private List<Cell> values;


    /** Caller must not modify the list that is passed here */
    public void setKeyValues(List<Cell> values) {
        this.values = values;
    }

    public void getKey(ImmutableBytesWritable ptr) {
        Cell value = values.get(0);
        ptr.set(value.getRowArray(), value.getRowOffset(), value.getRowLength());
    }

    public String toString() {
        return values.toString();
    }

    public int size() {
        return values.size();
    }

    public Cell getValue(int index) {
        return values.get(index);
    }
}
