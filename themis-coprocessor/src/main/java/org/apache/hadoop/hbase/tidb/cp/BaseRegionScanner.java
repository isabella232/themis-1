package org.apache.hadoop.hbase.tidb.cp;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * Created by shenli on 15/12/6.
 */
public abstract class BaseRegionScanner implements RegionScanner {
    @Override
    public boolean isFilterDone() {
        return false;
    }

    @Override
    public abstract boolean next(List<Cell> results) throws IOException;

    @Override
    public boolean next(List<Cell> result, int var2) throws IOException {
        return next(result);
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        throw new DoNotRetryIOException("Unsupported");
    }

    @Override
    public long getMvccReadPoint() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, int var2) throws IOException {
        return next(result, var2);
    }
}
