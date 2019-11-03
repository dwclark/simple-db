package simpledb.tx

import java.util.concurrent.atomic.AtomicInteger
import groovy.transform.CompileStatic
import simpledb.tx.recovery.*
import simpledb.tx.concurrency.*
import simpledb.log.LogManager
import simpledb.buffer.*
import simpledb.file.Block

@CompileStatic
class Transaction {
    private static final AtomicInteger txCounter = new AtomicInteger()

    private BufferManager bufferManager
    private RecoveryManager recoveryManager
    private ConcurrencyManager concurrencyManager
    private int txNumber
    private BufferList bufferList

    Transaction(final BufferManager bufferManager, final LogManager logManager, final LockTable lockTable) {
        this.txNumber = txCounter.incrementAndGet()
        this.bufferManager = bufferManager
        this.recoveryManager = new RecoveryManager(txNumber, logManager, bufferManager)
        this.concurrencyManager = new ConcurrencyManager(txNumber, lockTable)
        this.bufferList = new BufferList(bufferManager)
    }

    void commit() {
        bufferList.unpin()
        recoveryManager.commit()
        concurrencyManager.release()
        println "Committed transaction ${txNumber}"
    }

    void rollback() {
        bufferList.unpin()
        recoveryManager.rollback()
        concurrencyManager.release()
        println "Rolled back transaction ${txNumber}"
    }

    void recover() {
        recover()
        println "Finished recovery"
    }

    void pin(final Block block) {
        bufferList.pin(block)
    }

    void unpin(final Block block) {
        bufferList.unpin(block)
    }

    int getInt(final Block block, final int offset) {
        concurrencyManager.sharedLock(block)
        return bufferList.get(block).getInt(offset)
    }

    void setInt(final Block block, final int offset, final int val){
        concurrencyManager.exclusiveLock(block)
        final Buffer buffer = bufferList.get(block)
        final int lsn = recoveryManager.setInt(buffer, offset)
        buffer.setInt(offset, val, txNumber, lsn)
    }

    String getString(final Block block, final int offset) {
        concurrencyManager.sharedLock(block)
        return bufferList.get(block).getString(offset)
    }

    void setString(final Block block, final int offset, final String val) {
        concurrencyManager.exclusiveLock(block)
        final Buffer buffer = bufferList.get(block)
        final int lsn = recoveryManager.setString(buffer, offset)
        buffer.setString(offset, val, txNumber, lsn)
    }

    int size(final String fileName) {
        final Block dummy = Block.dummy(fileName)
        concurrencyManager.sharedLock(dummy)
        return bufferManager.fileManager.size(fileName)
    }

    Block append(final String fileName, final PageFormatter formatter) {
        Block dummy = Block.dummy(fileName)
        concurrencyManager.exclusiveLock(dummy)
        final Block block = bufferList.pin(fileName, formatter)
        unpin(block)
        return block
    }
}
