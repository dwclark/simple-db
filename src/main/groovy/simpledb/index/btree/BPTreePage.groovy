package simpledb.index.btree

import simpledb.file.*
import simpledb.record.TableInfo
import simpledb.tx.Transaction
import simpledb.query.*
import java.sql.Types

class BPTreePage {
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

    // int findSlotBefore(final Constant key) {
    //     int slot = 0
    //     while(slot < numberRecords && dataValue(slot) <=> key < 0) {
    //         ++slot
    //     }

    //     return slot - 1
    // }

    void close() {
        if(currentBlock) {
            tx.unpin(currentBlock)
        }

        currentBlock = null
    }

    int getNumberRecords() {
        return tx.getInt(currentBlock, Page.INT_SIZE)
    }

    boolean isFull() {
        return slotPosition(numberRecords + 1) >= tx.bufferManger.fileManager.pageSize
    }

    private void insert(int slot) {
        for(int i = numberRecords; i > slot; i--) {
            copyRecord(i-1, i)
        }

        numberRecords = numberRecords + 1
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
