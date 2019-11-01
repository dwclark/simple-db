package simpledb.buffer

import simpledb.file.*
import simpledb.log.LogManager
import groovy.transform.CompileStatic

@CompileStatic
class Buffer {

    private final LogManager logManager
    private final Page contents
    private Block block
    private int pins
    private int modifiedBy
    private int lsn

    Buffer(final FileManager fileManager, final LogManager logManager) {
        this.logManager = logManager
        contents = fileManager.newPage()
        block = Block.unassigned()
        pins = 0
        modifiedBy = -1
    }

    int getInt(final int offset) {
        return contents.getInt(offset)
    }

    String getString(final int offset) {
        return contents.getString(offset)
    }

    int setInt(final int offset, final int val, final int txNum, final int lsn) {
        return modify(txNum, lsn).setInt(offset, val)
    }

    int setString(final int offset, final String val, final int txNum, final int lsn) {
        return modify(txNum, lsn).setString(offset, val)
    }

    Block getBlock() {
        return block
    }

    void flush() {
        if(modifiedBy >= 0) {
            logManager.flush(lsn)
            contents.write(block)
        }

        modifiedBy = -1
    }

    void pin() {
        ++pins
    }

    void unpin() {
        --pins
    }

    boolean isPinned() {
        return pins > 0
    }

    boolean isModifiedBy(int txNum) {
        return txNum == modifiedBy
    }

    void assign(final Block block) {
        flush()
        this.block = block
        contents.read(block)
        pins = 0
    }

    void assign(final String fileName, final PageFormatter formatter) {
        flush()
        formatter.format(contents)
        block = contents.append(fileName)
        pins = 0
    }

    private Page modify(final int txNum, final int lsn) {
        modifiedBy = txNum
        if(lsn >= 0) {
            this.lsn = lsn
        }

        return contents
    }
}
