package simpledb.index.hash

import simpledb.index.Index
import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.query.TableScan
import simpledb.record.*
import simpledb.tx.Transaction

class HashIndex implements Index {
    final static int BUCKETS = 64
    final String indexName
    final Schema schema
    final Transaction tx

    private Constant searchKey
    private TableScan scan

    HashIndex(final String indexName, final Schema schema, final Transaction tx) {
        this.indexName = indexName
        this.schema = schema
        this.tx = tx
    }

    void beforeFirst(final Constant val) {
        close()
        searchKey = val
        int bucket = val.hashCode() % BUCKETS
        String tableName = "${indexName}${bucket}"
        scan = new TableScan(new TableInfo(tableName, schema), tx)
    }

    boolean next() {
        while(scan.next()) {
            if(scan.getVal('dataval') == searchKey) {
                return true
            }
        }

        return false
    }

    RID getDataRid() {
        int blockNumber = ts.getInt("block")
        int id = ts.getInt("id")
        return new RID(blockNumber, id)
    }

    void insert(final Constant val, final RID rid) {
        beforeFirst(val)
        ts.with {
            insert()
            setInt("block", rid.blockNumber)
            setInt("id", rid.id)
            setVal("dataval", val)
        }
    }

    void delete(final Constant val, final RID rid) {
        beforeFirst(val)
        while(next()) {
            if(dataRid == rid) {
                ts.delete()
                return
            }
        }
    }

    void close() {
        ts?.close()
    }

    static int searchCost(int numBlocks, int rpb){
        return numBlocks / BUCKETS
    }
}
