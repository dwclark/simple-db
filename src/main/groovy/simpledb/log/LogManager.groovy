package simpledb.log

import groovy.transform.*
import simpledb.file.*

@CompileStatic
class LogManager implements Iterable<BasicLogRecord> {
    static final int LAST_POS = 0
    
    final String logName
    final FileManager fileManager

    private Page page
    private Block currentBlock
    private int currentPosition
    
    LogManager(final String logName, final FileManager fileManager) {
        this.logName = logName
        this.fileManager = fileManager
        this.page = fileManager.newPage()
        int logSize = fileManager.size(logName)
        if(logSize == 0) {
            _block()
        }
        else {
            currentBlock = new Block(logName, logSize - 1)
            page.read(currentBlock)
            currentPosition = lastRecordPosition + Page.INT_SIZE
        }
    }

    Iterator<BasicLogRecord> iterator() {
        _flush()
        return new LogIterator(currentBlock, fileManager.newPage())
    }

    int getCurrentLSN() {
        return currentBlock.number
    }
    
    int getLastRecordPosition() {
        return page.getInt(LAST_POS)
    }

    void flush(final int lsn) {
        if(lsn >= currentLSN) {
            _flush()
        }
    }

    int append(final Object... rec) {
        int totalSize = Page.INT_SIZE
        rec.each { o -> totalSize += Page.length(o) }

        if(currentPosition + totalSize >= fileManager.pageSize) {
            _flush()
            _block()
        }

        rec.each { o -> currentPosition += page.setObject(currentPosition, o) }
        _finalize()
        return currentLSN
    }

    private void setLastRecordPosition(final int pos) {
        page.setInt(LAST_POS, pos)
    }

    private void _flush() {
        page.write(currentBlock)
    }

    private void _block() {
        lastRecordPosition = 0
        currentBlock = page.append(logName)
        currentPosition = Page.INT_SIZE
    }

    private void _finalize() {
        page.setInt(currentPosition, lastRecordPosition)
        lastRecordPosition = currentPosition
        currentPosition += Page.INT_SIZE
    }
}
