package org.apache.hadoop.hbase.tidb.cp;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpoint;
import org.apache.hadoop.hbase.themis.cp.ThemisServerScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TiDBScanObserver extends BaseRegionObserver {

    private final String TIDB_DIST_SQL_ENABLE = "_tidb_dist_sql_";

    private final String TIDB_SCAN_BY_ROW = "_tidb_scan_by_row_";

    // Will create TiDBScanner when 'tidb_scanner' is set in the attributes of scan;
    // otherwise, follow the origin read path to do hbase scan
    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
                                        final Scan scan, final RegionScanner s) throws IOException {
        if (!this.enableTiDBDistSQL(scan)) {
            return s;
        }
        try {

                ThemisCpUtil.prepareScan(scan, e.getEnvironment().getRegion().getTableDesc().getFamilies());
                checkFamily(e.getEnvironment().getRegion(), scan);
                ThemisEndpoint.checkReadTTL(System.currentTimeMillis(), themisStartTs,
                        PRE_SCANNER_OPEN_FEEK_ROW);
                Scan internalScan = ThemisCpUtil.constructLockAndWriteScan(scan, themisStartTs);
                ThemisServerScanner pScanner = new ThemisServerScanner(e.getEnvironment().getRegion()
                        .getScanner(internalScan), internalScan, themisStartTs, scan);
                e.bypass();
                return pScanner;
        } catch (Throwable ex) {
            throw new DoNotRetryIOException("themis exception in tidb postScannerOpen", ex);
        }
    }

    private boolean enableTiDBDistSQL(final Scan scan) {
        byte[] bs = scan.getAttribute(this.TIDB_DIST_SQL_ENABLE);
        return bs == null ? false : Bytes.toBoolean(bs);
    }

    private RegionScanner wrapRowScanner(final ObserverContext<RegionCoprocessorEnvironment> e,
                                         final Scan scan, final RegionScanner s) {
        byte[] bs = scan.getAttribute(this.TIDB_SCAN_BY_ROW);
        boolean r = bs == null ? false : Bytes.toBoolean(bs);
        if (bs == null || !Bytes.toBoolean(bs)) {
            return s;
        }
        TiDBTableScanner tscanner = new TiDBTableScanner(s, scan);
    }
}
