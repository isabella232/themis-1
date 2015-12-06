package org.apache.hadoop.hbase.tidb.cp;

import com.pingcap.tidb.expression.aggregate.Aggregator;
import com.pingcap.tidb.expression.aggregate.Aggregators;
import com.pingcap.tidb.expression.aggregate.CountAggregator;
import com.pingcap.tidb.expression.aggregate.ServerAggregators;
import com.pingcap.tidb.schema.TRow;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TiDBRegionServerObserver extends BaseRegionObserver {

    public static final String TIDB_DIST_SQL_ENABLE = "_tidb_dist_sql_";
    public static final String TIDB_SCAN_BY_ROW = "_tidb_scan_by_row_";
    public static final String TIDB_AGGS_ENABLE = "_tidb_aggs_enable_";
    public static final String TIDB_AGGREGATOR = "_tidb_aggregator_";

    // Will create TiDBScanner when 'tidb_scanner' is set in the attributes of scan;
    // otherwise, follow the origin read path to do hbase scan
    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
                                        final Scan scan, final RegionScanner s) throws IOException {
        if (!this.hasTiDBFlag(scan, this.TIDB_DIST_SQL_ENABLE)) {
            return s;
        }
        try {
            return buildTiDBScanner(e, scan, s);
        } catch (Throwable ex) {
            throw new DoNotRetryIOException("themis exception in tidb postScannerOpen", ex);
        }
    }

    private boolean hasTiDBFlag(final Scan scan, final String flag) {
        byte[] bs = scan.getAttribute(flag);
        return bs == null ? false : Bytes.toBoolean(bs);
    }


    private RegionScanner buildTiDBScanner(final ObserverContext<RegionCoprocessorEnvironment> e,
                                         final Scan scan, final RegionScanner s) throws IOException {
        if (!hasTiDBFlag(scan, this.TIDB_SCAN_BY_ROW)) {
            return s;
        }
        TiDBTableScanner ts = new TiDBTableScanner(s, scan);
        if (!hasTiDBFlag(scan, this.TIDB_AGGS_ENABLE)) {
            return ts;
        }
        return wrapAggScanner(e, scan, ts);
    }

    private RegionScanner wrapAggScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                         final Scan scan, final RegionScanner innerScanner) throws IOException {
        HRegion region = c.getEnvironment().getRegion();
        region.startRegionOperation();

        CountAggregator aggregator = new CountAggregator();
        // Get Agg function.
        boolean hasMore;
        boolean hasAny = false;

        try {
            synchronized (innerScanner) {
                do {
                    List<Cell> results = new ArrayList<Cell>();
                    hasMore = innerScanner.nextRaw(results);
                    if (results.isEmpty()) {
                        break;
                    }
                    TRow row = new TRow();
                    row.setKeyValues(results);
                    /// aggregate row
                    aggregator.aggregate(row, null);
                    hasAny = true;
                } while (hasMore);
            }
        } catch (Exception ex) {

        }

        final boolean hadAny = hasAny;
        KeyValue keyValue = null;
        if (hadAny) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            aggregator.evaluate(null, ptr);
            byte[] value = ptr.get();
            byte[] AGG_ROW_KEY = Bytes.toBytes("a");
            byte[] AGG_CF = Bytes.toBytes("acf");
            byte[] AGG_CQ = Bytes.toBytes("acq");
            keyValue = new KeyValue(AGG_ROW_KEY, 0, AGG_ROW_KEY.length,
                    AGG_CF, 0, AGG_CF.length,
                    AGG_CQ, 0, AGG_CQ.length,
                    0, KeyValue.Type.Put,
                    value, 0, value.length);
        }

        final KeyValue aggKeyValue = keyValue;

        RegionScanner scanner = new BaseRegionScanner() {
            private boolean done = !hadAny;

            @Override
            public HRegionInfo getRegionInfo() {
                return innerScanner.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() {
                return done;
            }

            @Override
            public void close() throws IOException {
                innerScanner.close();
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                if (done) return false;
                done = true;
                results.add(aggKeyValue);
                return false;
            }

            @Override
            public long getMaxResultSize() {
                return scan.getMaxResultSize();
            }
        };
        return scanner;
    }
}
