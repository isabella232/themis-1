package org.apache.hadoop.hbase.tidb.cp;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.hadoop.hbase.tidb.cp.generated.TiDBProto;
import org.apache.hadoop.hbase.tidb.tables.TTable;

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

    private String currentRowKey;

    private List<Cell> currentRow;


    public TiDBTableScanner(RegionScanner scanner) {
        this(scanner, null);
    }

    public TiDBTableScanner(RegionScanner scanner, Scan scan) {
        this.scan = scan;
        this.scanner = scanner;
        this.table = getTableInfo(scan);
        /*
        if (this.table == nil) {
            panic
            throw error
        }*/
    }


    private TTable getTableInfo(Scan scan) {
        byte[] bs = scan.getAttribute("_tidb_table_info_");
        if (bs == Null) {
            return Null;
        }
        try {
            return new TTable(TiDBProto.TTable.parseFrom(bs));
        } catch (InvalidProtocolBufferException e){
            return Null;
        }
    }

    public Scan getScan() {
        return this.scan;
    }

    public boolean next(List<Cell> results) throws IOException {
        List<Cell> innerResult = new ArrayList<Cell>();
        boolean hasMore = scanner.nextRaw(innerResult);
        if (!hasMore && innerResult.isEmpty()) {
            results.addAll(this.currentRow);
            this.currentRow.clear();
            return false;
        }
        Cell firstCell = innerResult.get(0);
        firstCell.getRowArray();
        //String key = Bytes.toString(this.scanner.);
        /*
         TODO: Get next row and continue while we get the entire tidb row
         row = this.currentRow
         string key = Bytes.toString(this.scanner.getRow())
         while (this.currentRowKey == "" || key == this.currentRowKey) {
            row.append(results[0])
            currentRowKey = key
         }
         this.currentRow.Clear()
         this.currentRow.append(results[0])
         this.currentRowKey = key
         return row
         */
        return scanner.next(results);
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
