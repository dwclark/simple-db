package simpledb.index.btree

import groovy.transform.CompileStatic
import simpledb.buffer.PageFormatter
import simpledb.file.*
import simpledb.record.*
import simpledb.tx.Transaction
import simpledb.query.*
import java.sql.Types

@CompileStatic
class BPTreeLeaf {
    final TableInfo ti
    final Transaction tx
    final String fileName
    final Constant searchKey

    private BPTreePage contents
    private int currentSlot

    BPTreeLeaf(final Block block, final TableInfo ti, final Constant searchKey, final Transaction tx) {
        this.ti = ti
        this.tx = tx
        this.searchKey = searchKey
        fileName = block.fileName
        contents = new BPTreePage(block, ti, tx)
        currentSlot = contents.findSlotBefore(searchKey)
    }

    void close() {
        contents.close()
    }

    boolean next() {
        ++currentSlot
        if(currentSlot >= contents.numberRecords) {
            return tryOverflow()
        }
        else if(contents.getDataVal(currentSlot) == searchKey) {
            return true
        }
        else {
            return tryOverflow()
        }
    }

    RID getDataRid() {
        return contents.getDataRid(currentSlot)
    }

    void delete(RID rid) {
        while(next()) {
            if(dataRid == rid) {
                contents.delete(currentSlot)
                return
            }
        }
    }

    DirEntry insert(RID rid) {
        ++currentSlot
        contents.insertLeaf(currentSlot, searchKey, rid)
        if(!contents.full) {
            return null
        }

        final Constant firstKey = contents.getDataVal(0)
        final Constant lastKey = contents.getDataVal(contents.numberRecords - 1)
        if(lastKey == firstKey) {
            final Block newBlock = contents.split(1, contents.flag)
            contents.flag = newBlock.number
            return null
        }
        else {
            int splitPosition = (int) (contents.numberRecords / 2)
            Constant splitKey = contents.getDataVal(splitPosition)
            if(splitKey == firstKey) {
                while(contents.getDataVal(splitPosition) == splitKey) {
                    ++splitPosition
                }

                splitKey = contents.getDataVal(splitPosition)
            }
            else {
                while(contents.getDataVal(splitPosition - 1) == splitKey) {
                    --splitPosition
                }
            }

            Block newBlock = contents.split(splitPosition, -1)
            return new DirEntry(splitKey, newBlock.number)
        }
    }

    private boolean tryOverflow() {
        final Constant firstKey = contents.getDataVal(0)
        final int flag = contents.flag
        if(searchKey != firstKey || flag < 0) {
            return false
        }

        contents.close()
        Block nextBlock = new Block(fileName, flag)
        contents = new BPTreePage(nextBlock, ti, tx)
        currentSlot = 0
        return true
    }
}
