package simpledb.tx.recovery

import groovy.transform.CompileStatic
import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.file.Block
import simpledb.log.LogManager

@CompileStatic
class RecoveryManager {

    final int txNumber
    final LogManager logManager
    final BufferManager bufferManager
    
    RecoveryManager(final int txNumber, final LogManager logManager, final BufferManager bufferManager) {
        this.txNumber = txNumber
        this.logManager = logManager
        this.bufferManager = bufferManager

        new StartRecord(txNumber).write(logManager)
    }

    void commit() {
        bufferManager.flushAll(txNumber)
        final int lsn = new CommitRecord(txNumber).write(logManager)
        logManager.flush(lsn)
    }

    void rollback() {
        doRollback()
        bufferManager.flushAll(txNumber)
        final int lsn = new RollbackRecord(txNumber).write(logManager)
        logManager.flush(lsn)
    }

    void recover() {
        doRecover()
        bufferManager.flushAll(txNumber)
        final int lsn = new CheckpointRecord().write(logManager)
        logManager.flush(lsn)
    }

    int setInt(final Buffer buffer, final int offset) {
        final int oldVal = buffer.getInt(offset)
        final Block block = buffer.block
        if(block.temporary) {
            return -1
        }
        else {
            new SetIntRecord(txNumber, block, offset, oldVal).write(logManager)
        }
    }

    int setString(final Buffer buffer, final int offset) {
        final String oldVal = buffer.getString(offset)
        final Block block = buffer.block
        if(block.temporary) {
            return -1
        }
        else {
            new SetStringRecord(txNumber, block, offset, oldVal).write(logManager)
        }
    }

    private void doRollback() {
        Iterator<LogRecord> iter = new LogRecordIterator(logManager)
        while(iter.hasNext()) {
            LogRecord rec = iter.next()
            if(rec.txNumber == txNumber) {
                if(rec.type == LogType.START){
                    return;
                }

                rec.undo(bufferManager, txNumber)
            }
        }
    }

    private void doRecover() {
        Set<Integer> committed = new HashSet<>()
        Iterator<LogRecord> iter = new LogRecordIterator(logManager)
        while(iter.hasNext()) {
            LogRecord rec = iter.next()
            if(rec.type == LogType.CHECKPOINT) {
                return;
            }

            if(rec.type == LogType.COMMIT) {
                committed.add(rec.txNumber)
            }
            else if(!committed.contains(rec.txNumber)) {
                rec.undo(bufferManager, rec.txNumber)
            }
        }
    }
}
