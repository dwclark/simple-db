package simpledb.index.btree

import groovy.transform.CompileStatic
import java.sql.Types
import simpledb.buffer.PageFormatter
import simpledb.file.*
import simpledb.index.Index
import simpledb.query.*
import simpledb.record.*
import simpledb.tx.Transaction

@CompileStatic
class BPTreeIndex implements Index {
    final Transaction tx
    final TableInfo dirTi, leafTi

    private BPTreeLeaf leaf = null
    private Block rootBlock

    BPTreeIndex(final String indexName, final Schema leafSchema, final Transaction tx) {
        this.tx = tx
        String leafTable = indexName + "leaf"
        leafTi =new TableInfo(leafTable, leafSchema)
        if(tx.size(leafTi.fileName) == 0) {
            tx.append(leafTi.fileName, new BPTreeRecordFormatter(-1, leafTi, tx.bufferManager.fileManager.pageSize))
        }

        Schema dirSchema = Schema.fromFields([leafSchema.field("block"), leafSchema.field("dataval")])
        String dirTable = indexName + "dir"
        dirTi = new TableInfo(dirTable, dirSchema)
        rootBlock = new Block(dirTi.fileName, 0)
        if(tx.size(dirTi.fileName) == 0) {
            tx.append(dirTi.fileName, new BPTreeRecordFormatter(0, dirTi, tx.bufferManager.fileManager.pageSize))
        }

        BPTreePage page = new BPTreePage(rootBlock, dirTi, tx)
        if(page.numberRecords == 0) {
            int type = dirSchema.field("dataval").type
            Constant minVal = (type == Types.INTEGER) ? new IntConstant(Integer.MIN_VALUE) : new StringConstant("")
            page.insertDir(0, minVal, 0)
        }

        page.close()
    }

    void beforeFirst(final Constant key) {
        close()
        BPTreeDir root = new BPTreeDir(rootBlock, dirTi, tx)
        int blockNumber = root.search(key)
        root.close()
        Block leafBlock = new Block(leafTi.fileName, blockNumber)
        this.leaf = new BPTreeLeaf(leafBlock, leafTi, key, tx)
    }
    
    boolean next() {
        return leaf.next()
    }
    
    RID getDataRid() {
        return leaf.dataRid
    }
    
    void insert(final Constant val, final RID rid) {
        beforeFirst val
        DirEntry e = leaf.insert(rid)
        leaf.close()
        if(e == null) {
            return
        }

        BPTreeDir root = new BPTreeDir(rootBlock, dirTi, tx)
        DirEntry e2 = root.insert(e)
        if(e2 != null) {
            root.makeNewRoot(e2)
        }

        root.close()
    }
    
    void delete(final Constant val, final RID rid) {
        beforeFirst val
        leaf.delete rid
        leaf.close()
    }
    
    void close() {
        leaf?.close()
    }

    static int searchCost(int numberBlocks, int rpb) {
        return 1 + (int) (Math.log(numberBlocks) / Math.log(rpb))
    }
}
