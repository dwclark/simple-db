package simpledb.index.btree

import groovy.transform.CompileStatic
import simpledb.buffer.PageFormatter
import simpledb.file.*
import simpledb.record.*
import simpledb.tx.Transaction
import simpledb.query.*
import java.sql.Types

@CompileStatic
class BPTreeDir {
    final TableInfo ti
    final Transaction tx
    final String fileName

    private BPTreePage contents

    BPTreeDir(final Block block, final TableInfo ti, final Transaction tx) {
        this.ti = ti
        this.tx = tx
        this.fileName = block.fileName
        this.contents = new BPTreePage(block, ti, tx)
    }

    void close() {
        contents.close()
    }

    int search(final Constant searchKey) {
        Block childBlock = findChildBlock(searchKey)
        while(contents.flag > 0) {
            contents.close()
            contents = new BPTreePage(childBlock, ti, tx)
            childBlock = findChildBlock(searchKey)
        }

        return childBlock.number
    }

    void makeNewRoot(final DirEntry e) {
        Constant firstVal = contents.getDataVal(0)
        int level = contents.flag
        Block newBlock = contents.split(0, level)
        DirEntry oldRoot = new DirEntry(firstVal, newBlock.number)
        insertEntry(oldRoot)
        insertEntry(e)
        contents.flag = level + 1
    }

    DirEntry insert(final DirEntry e) {
        if(contents.flag == 0) {
            return insertEntry(e)
        }

        Block childBlock = findChildBlock(e.dataVal)
        BPTreeDir child = new BPTreeDir(childBlock, ti, tx)
        DirEntry myEntry = child.insert(e)
        child.close()
        return (myEntry != null) ? insertEntry(myEntry) : null
    }

    private DirEntry insertEntry(final DirEntry e) {
        int newSlot = 1 + contents.findSlotBefore(e.dataVal)
        contents.insertDir(newSlot, e.dataVal, e.blockNumber)
        if(!contents.full) {
            return null
        }

        int level = contents.flag
        int pos = (int) (contents.numberRecords / 2)
        Constant val = contents.getDataVal(pos)
        Block newBlock = contents.split(pos, level)
        return new DirEntry(val, newBlock.number)
    }

    private Block findChildBlock(final Constant searchKey) {
        int slot = contents.findSlotBefore(searchKey)
        if(contents.getDataVal(slot+1) == searchKey) {
            ++slot
        }

        int blockNumber = contents.childNumber(slot)
        return new Block(fileName, blockNumber)
    }
}
