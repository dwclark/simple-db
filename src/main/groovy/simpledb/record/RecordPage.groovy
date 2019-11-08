package simpledb.record

import groovy.transform.*
import simpledb.file.Page
import simpledb.tx.Transaction
import simpledb.file.Block

@CompileStatic
class RecordPage {
    static final int EMPTY = 0
    static final int IN_USE = 1

    private final TableInfo tableInfo
    private final Transaction tx
    private final int slotSize
    private final int pageSize
    private Block block

    int currentSlot

    RecordPage(final Block block, final TableInfo tableInfo,
               final Transaction tx) {
        this.block = block
        this.tableInfo = tableInfo
        this.tx = tx
        this.slotSize = Page.INT_SIZE + tableInfo.recordLength
        this.pageSize = tx.bufferManager.fileManager.pageSize
        this.currentSlot = -1

        tx.pin(block)
    }

    void close() {
        if(block != null) {
            tx.unpin(block)
        }

        block = null
    }

    boolean next() {
        return searchFor(IN_USE)
    }

    int getInt(final String name) {
        return tx.getInt(block, fieldPosition(name))
    }

    void setInt(final String name, final int val) {
        tx.setInt(block, fieldPosition(name), val)
    }

    String getString(final String name) {
        return tx.getString(block, fieldPosition(name))
    }

    void setString(final String name, final String val) {
        tx.setString(block, fieldPosition(name), val)
    }

    void delete() {
        tx.setInt(block, currentPosition, EMPTY)
    }

    boolean insert() {
        currentSlot = -1
        if(searchFor(EMPTY)) {
            tx.setInt(block, currentPosition, IN_USE)
            return true
        }
        else {
            return false
        }
    }

    private int fieldPosition(final String name) {
        return currentPosition + Page.INT_SIZE + tableInfo.offset(name)
    }

    private int getCurrentPosition() {
        return currentSlot * slotSize
    }

    private boolean isValidSlot() {
        return currentPosition + slotSize <= pageSize
    }

    private boolean searchFor(final int flag) {
        ++currentSlot
        while(validSlot) {
            final int position = currentPosition
            if(tx.getInt(block, position) == flag) {
                return true
            }

            ++currentSlot
        }

        return false
    }
}
