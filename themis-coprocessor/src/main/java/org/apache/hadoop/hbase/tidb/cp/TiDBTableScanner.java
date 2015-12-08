package org.apache.hadoop.hbase.tidb.cp;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.hadoop.hbase.tidb.cp.generated.TiDBProto;
import org.apache.hadoop.hbase.tidb.tables.TTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CellUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by shenli on 15-11-25.
 */
public class TiDBTableScanner implements RegionScanner {
    private final Scan scan;
    private final RegionScanner scanner;
    private final TTable table;
    private String currentRowKey;
    private List<Cell> currentRow;

    private static final Log LOG = LogFactory.getLog(TiDBTableScanner.class);

    public TiDBTableScanner(RegionScanner scanner) {
        this(scanner, null);
    }

    public TiDBTableScanner(RegionScanner scanner, Scan scan) {
        this.scan = scan;
        this.scanner = scanner;
        this.table = getTableInfo(scan);
        if (this.table == null) {
            System.err.println("Can not find tidb table info in table scanner!!!");
        }
        this.currentRowKey = "";
        this.currentRow = new ArrayList<>();
    }

    private TTable getTableInfo(Scan scan) {
        byte[] bs = scan.getAttribute("_tidb_table_info_");
        if (bs == null) {
            return null;
        }
        try {
            return new TTable(TiDBProto.TTable.parseFrom(bs));
        } catch (InvalidProtocolBufferException e){
            return null;
        }
    }

    public Scan getScan() {
        return this.scan;
    }

    private String getRowKey(Cell c) {
        byte[] buffer = new byte[c.getRowLength()];
        CellUtil.copyRowTo(c, buffer, 0);
        return Bytes.toString(buffer);
    }

    /*
        Returns next row for tidb table.
     */
    @Override
    public boolean next(List<Cell> results) throws IOException {
        LOG.info("Enter next");
        boolean hasMore = true;
        while (true) {
            List<Cell> innerResult = new ArrayList<Cell>();
            hasMore = scanner.next(innerResult);
            LOG.info("Find a raw row, hasMore " + hasMore);
            if (innerResult.isEmpty()) {
                // No result.
                results.addAll(this.currentRow);
                hasMore = false;
                break;
            }

            // Get Rowkey
            Cell data = innerResult.get(0);
            //String rowKey = this.getRowKey(data);
            String rowKey = Bytes.toString(data.getRow());
            LOG.info("RowKey " + rowKey);

            // Check if out of table range
            if (!this.table.IsSameTable(rowKey)) {
                results.addAll(this.currentRow);
                hasMore = false;
                LOG.info("Find the last row in the table");
                break;
            }
            LOG.info("Find a row in this table");
            // A new row?
            if (this.currentRowKey.length() == 0) {
                this.currentRowKey = rowKey;
                LOG.info("Current RowKey: " + rowKey);
            }
            if (!rowKey.startsWith(this.currentRowKey)) {
                // Find a new row, returns the current row
                results.addAll(this.currentRow);
                this.currentRow.clear();
                this.currentRow.add(data);
                LOG.info("Find a new row");
                this.currentRowKey = rowKey;
                break;
            }

            if (!hasMore) {
                // Last column in the row.
                this.currentRow.add(data);
                results.addAll(this.currentRow);
                LOG.info("A new Row");
                break;
            }
            // add column data in the row
            this.currentRow.add(data);
        }
        //results.addAll(this.currentRow);
        LOG.info("Leave next, hasMore:" + hasMore);
        this.currentRowKey = "";
        return hasMore;
    }

    public boolean next(List<Cell> result, int limit) throws IOException {
        return scanner.next(result, limit);
    }

    public long getMvccReadPoint() {
        return scanner.getMvccReadPoint();
    }

    public void close() throws IOException {
        scanner.close();
    }

    public HRegionInfo getRegionInfo() {
        return scanner.getRegionInfo();
    }

    public boolean isFilterDone() throws IOException {
        return scanner.isFilterDone();
    }

    public boolean reseek(byte[] row) throws IOException {
        return scanner.reseek(row);
    }

    public long getMaxResultSize() {
        return scanner.getMaxResultSize();
    }

    public boolean nextRaw(List<Cell> result) throws IOException {
        return scanner.nextRaw(result);
    }

    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return scanner.nextRaw(result, limit);
    }
}
