package org.apache.hadoop.hbase.tidb.cp;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.hadoop.hbase.tidb.cp.generated.TiDBProto;
import org.apache.hadoop.hbase.tidb.tables.TTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shenli on 15-11-25.
 */
public class TiDBTableScanner implements RegionScanner {
    private final Scan scan;
    private final RegionScanner scanner;

    private final TTable table;

    private String tablePrefix;

    private String currentRowKey;

    private List<Cell> currentRow;

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


    /*
        Returns next row for tidb table.
     */
    @Override
    public boolean next(List<Cell> results) throws IOException {
        boolean hasMore = true;
        while (true) {
            List<Cell> innerResult = new ArrayList<Cell>();
            hasMore = scanner.nextRaw(innerResult);
            if (innerResult.isEmpty()) {
                // No result.
                hasMore = false;
                break;
            }
            Cell data = innerResult.get(0);
            String rowKey = Bytes.toString(data.getRowArray());
            if (!this.table.IsSameTable(rowKey)) {
                hasMore = false;
                break;
            }

            if (!hasMore) {
                // Last result.
                this.currentRow.add(data);
                break;
            }
            if (this.currentRowKey != "" && this.currentRowKey != rowKey) {
                // Find a new row, returns the current row
                results.addAll(this.currentRow);
                this.currentRow.clear();
                this.currentRow.add(data);
                this.currentRowKey = rowKey;
                break;
            }
            this.currentRow.add(data);
            this.currentRowKey = rowKey;
        }
        results.addAll(this.currentRow);
        this.currentRow.clear();
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
