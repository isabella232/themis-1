package org.apache.hadoop.hbase.tidb.cp;

import com.pingcap.tidb.expression.aggregate.Aggregators;
import com.pingcap.tidb.expression.aggregate.ServerAggregators;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class TiDBRegionServerObserver extends BaseRegionObserver {

    public static final String TIDB_DIST_SQL_ENABLE = "_tidb_dist_sql_";
    public static final String TIDB_SCAN_BY_ROW = "_tidb_scan_by_row_";
    public static final String TIDB_AGGS_ENABLE = "_tidb_aggs_enable_";
    public static final String TIDB_AGGREGATORS = "_tidb_aggregators_";
    public static final String UNORDERED_GROUP_BY_EXPRESSIONS = "_UnorderedGroupByExpressions";
    public static final String KEY_ORDERED_GROUP_BY_EXPRESSIONS = "_OrderedGroupByExpressions";
    public static final String ESTIMATED_DISTINCT_VALUES = "_EstDistinctValues";
    public static final String NON_AGGREGATE_QUERY = "_NonAggregateQuery";
    public static final String TOPN = "_TopN";
    public static final String UNGROUPED_AGGS = "_UngroupedAggs";
    public static final String DELETE_AGG = "_DeleteAgg";

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
        if (!hasTiDBFlag(scan, this.TIDB_AGGS)) {
            return ts;
        }
        return wrapAggScanner(e, scan, ts);
    }

    private RegionScanner wrapAggScanner(final ObserverContext<RegionCoprocessorEnvironment> e,
                                         final Scan scan, final RegionScanner innerScanner) throws IOException {

        HRegion region = c.getEnvironment().getRegion();
        region.startRegionOperation();

        KeyValue aggKeyValue = new KeyValue();
        // Get Agg function.
        Aggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(this.TIDB_AGGREGATORS), c.getEnvironment().getConfiguration());


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
