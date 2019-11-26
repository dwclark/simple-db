package simpledb.index.btree

import groovy.transform.CompileStatic
import simpledb.buffer.PageFormatter
import simpledb.file.*
import simpledb.record.*
import simpledb.tx.Transaction
import simpledb.query.*
import java.sql.Types

@CompileStatic
class BPTreePage {
    private static final String BLOCK = "block"
    private static final String ID = "id"
    private static final String DATAVAL = "dataval"
    
    final TableInfo ti
    final Transaction tx
    final int slotSize
    
    private Block currentBlock

    BPTreePage(final Block currentBlock, final TableInfo ti, final Transaction tx) {
        this.currentBlock = currentBlock
        this.ti = ti
        this.tx = tx
        this.slotSize = ti.recordLength
        tx.pin(currentBlock)
    }

    int findSlotBefore(final Constant key) {
        int slot = 0
        while(slot < numberRecords && ((getDataVal(slot) <=> key) < 0)) {
            ++slot
        }

        return slot - 1
    }

    void close() {
        if(currentBlock) {
            tx.unpin(currentBlock)
        }

        currentBlock = null
    }

    boolean isFull() {
        return slotPosition(numberRecords + 1) >= tx.bufferManager.fileManager.pageSize
    }

    Block split(final int splitPos, final int flag) {
        Block newBlock = append(flag)
        BPTreePage ret = new BPTreePage(newBlock, ti, tx)
        transfer splitPos, ret
        ret.flag = flag
        ret.close()
        return newBlock
    }

    Constant getDataVal(final int slot) {
        return getVal(slot, DATAVAL)
    }

    void setFlag(final int val) {
        tx.setInt currentBlock , 0, val
    }

    int getFlag() {
        return tx.getInt(currentBlock, 0)
    }

    Block append(final int flag) {
        final int pageSize = tx.bufferManager.fileManager.pageSize
        return tx.append(ti.fileName, new BPTreeRecordFormatter(flag, ti, pageSize))
    }

    int childNumber(final int slot) {
        return getInt(slot, BLOCK)
    }
    
    void insertDir(final int slot, final Constant val, final int blockNumber) {
        insert slot
        setVal slot, DATAVAL, val
        setInt slot, BLOCK, blockNumber
    }

    RID getDataRid(final int slot) {
        return new RID(getInt(slot, BLOCK), getInt(slot, ID))
    }
    
    void insertLeaf(final int slot, final Constant val, final RID rid) {
        insert slot
        setVal slot, DATAVAL, val
        setInt slot, BLOCK, rid.blockNumber
        setInt slot, ID, rid.id
    }
    
    void delete(final int slot){
        for(int i = slot + 1; i < numberRecords; ++i) {
            copy(i, i-1)
        }

        numberRecords = numberRecords - 1
    }

    int getNumberRecords() {
        return tx.getInt(currentBlock, Page.INT_SIZE)
    }

    private void insert(int slot) {
        for(int i = numberRecords; i > slot; i--) {
            copy(i-1, i)
        }

        numberRecords = numberRecords + 1
    }

    private void transfer(final int slot, final BPTreePage dest) {
        int destSlot = 0
        while(slot < numberRecords) {
            dest.insert(destSlot)
            ti.schema.fieldNames.each { String name ->
                dest.setVal(destSlot, name, getVal(slot, name))
            }

            delete(slot)
            ++destSlot
        }
    }

    private void copy(final int from, final int to) {
        ti.schema.fieldNames.each { String name -> setVal(to, name, getVal(from, name)) }
    }

    private Constant getVal(final int slot, final String fieldName) {
        final int type = ti.schema.field(fieldName).type
        if(type == Types.INTEGER) {
            return new IntConstant(getInt(slot, fieldName))
        }
        else if(type == Types.VARCHAR) {
            return new StringConstant(getString(slot, fieldName))
        }
        else {
            throw new UnsupportedOperationException()
        }
    }

    private void setVal(final int slot, final String fieldName, final Constant val) {
        final int type = ti.schema.field(fieldName).type
        if(type == Types.INTEGER) {
            setInt(slot, fieldName, ((IntConstant) val).val)
        }
        else if(type == Types.VARCHAR) {
            setString(slot, fieldName, ((StringConstant) val).val)
        }
        else {
            throw new UnsupportedOperationException()
        }
    }

    private int getInt(final int slot, final String field) {
        return tx.getInt(currentBlock, fieldPosition(slot, field))
    }

    private void setInt(final int slot, final String field, final int val) {
        tx.setInt(currentBlock, fieldPosition(slot, field), val)
    }

    private String getString(final int slot, final String field) {
        return tx.getString(currentBlock, fieldPosition(slot, field))
    }

    private void setString(final int slot, final String field, final String val) {
        tx.setString(currentBlock, fieldPosition(slot, field), val)
    }

    private void setNumberRecords(final int n) {
        tx.setInt(currentBlock, Page.INT_SIZE, n)
    }
    
    private int fieldPosition(final int slot, final String field) {
        return slotPosition(slot) + ti.offset(field)
    }
    
    private int slotPosition(final int slot) {
        return Page.INT_SIZE + Page.INT_SIZE + (slot * slotSize)
    }
}

@CompileStatic
protected class BPTreeRecordFormatter implements PageFormatter {
    final TableInfo tableInfo
    final int pageSize
    final int flag
    
    BPTreeRecordFormatter(final int flag, final TableInfo tableInfo, final int pageSize) {
        this.flag = flag
        this.tableInfo = tableInfo
        this.pageSize = pageSize
    }

    void format(final Page page) {
        page.setInt 0, flag
        page.setInt Integer.BYTES, 0
        int recSize = tableInfo.recordLength
        for(int pos = 2 * Integer.BYTES; pos + recSize <= pageSize; pos += recSize) {
            defaultRecord page, pos
        }
    }

    private void defaultRecord(final Page page, final int pos) {
        tableInfo.schema.fields.each { Field field ->
            final int writeAt = pos + tableInfo.offset(field.name)
            if(field.type == Types.INTEGER) {
                page.setInt writeAt, 0
            }
            else if(field.type == Types.VARCHAR) {
                page.setString writeAt, ""
            }
        }
    }
}
