package simpledb.index.hash

import java.util.function.IntBinaryOperator
import simpledb.index.Index
import simpledb.index.IndexFactory
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
            if(scan.getVal(DATAVAL) == searchKey) {
                return true
            }
        }

        return false
    }

    RID getDataRid() {
        int blockNumber = ts.getInt(BLOCK)
        int id = ts.getInt(ID)
        return new RID(blockNumber, id)
    }

    void insert(final Constant val, final RID rid) {
        beforeFirst(val)
        ts.with {
            insert()
            setInt(BLOCK, rid.blockNumber)
            setInt(ID, rid.id)
            setVal(DATAVAL, val)
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

    final static IntBinaryOperator COST = new IntBinaryOperator() {
        int applyAsInt(final int numberBlocks, final int rpb) {
            return numberBlocks / BUCKETS
        }
    }

    final static IndexFactory FACTORY = new IndexFactory() {
        Index create(final String indexName, final Schema schema, final Transaction tx) {
            return new HashIndex(indexName, schema, tx)
        }
    }
}
