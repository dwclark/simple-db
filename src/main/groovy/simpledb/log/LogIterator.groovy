package simpledb.log

import simpledb.file.Page
import simpledb.file.Block
import groovy.transform.CompileStatic

@CompileStatic
class LogIterator implements Iterator<BasicLogRecord> {

    final Page page
    private Block block
    private int current
    
    LogIterator(final Block startBlock, final Page page) {
        this.block = startBlock
        this.page = page
        readBlock()
    }

    boolean hasNext() {
        return current > 0 || block.number > 0
    }

    BasicLogRecord next() {
        if(current == 0) {
            nextBlock()
        }

        current = page.getInt(current)
        return new BasicLogRecord(page, current + Page.INT_SIZE)
    }

    void remove() {
        throw new UnsupportedOperationException()
    }

    private void nextBlock() {
        block = new Block(block.fileName, block.number - 1)
        readBlock()
    }

    private void readBlock(){
        page.read(block)
        current = page.getInt(LogManager.LAST_POS)
    }
}
