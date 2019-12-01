package simpledb.index

import groovy.transform.CompileStatic
import simpledb.query.*

@CompileStatic
class IndexSelectScan implements Scan {
    final Index index
    final Constant val
    @Delegate private final TableScan tableScan

    IndexSelectScan(final Index index, final Constant val, final TableScan tableScan) {
        this.index = index
        this.val = val
        this.tableScan = tableScan
    }

    void beforeFirst() {
        index.beforeFirst(val)
    }

    boolean next() {
        if(index.next()) {
            tableScan.moveToRid(index.dataRid)
            return true
        }
        else {
            return false
        }
    }

    void close() {
        index.close()
        tableScan.close()
    }
}
