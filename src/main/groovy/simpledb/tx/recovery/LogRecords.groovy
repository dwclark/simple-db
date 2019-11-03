package simpledb.tx.recovery

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.ToString
import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.file.Block
import simpledb.log.BasicLogRecord
import simpledb.log.LogManager

@CompileStatic
@ToString(includePackage=false, includeNames=true)
class SetStringRecord implements LogRecord {
    final int txNumber
    final Block block
    final int offset
    final String val

    LogType getType() {
        return LogType.SETSTRING
    }

    SetStringRecord(final int txNumber, final Block block,
                    final int offset, final String val) {
        this.txNumber = txNumber
        this.block = block
        this.offset = offset
        this.val = val
    }

    SetStringRecord(final BasicLogRecord rec) {
        this.txNumber = rec.nextInt()
        this.block = new Block(rec.nextString(), rec.nextInt())
        this.offset = rec.nextInt()
        this.val = rec.nextString()
    }

    int write(final LogManager logManager) {
        return logManager.append(type.id, txNumber, block.fileName, block.number, offset, val)
    }
        
    void undo(final BufferManager bufferManager, final int txNumber) {
        bufferManager.withBuffer(block) { Buffer buffer ->
            buffer.setString(offset, val, txNumber, -1)
        }
    }
}

@CompileStatic
@ToString(includePackage=false, includeNames=true)
class SetIntRecord implements LogRecord {
    final int txNumber
    final Block block
    final int offset
    final int val

    LogType getType() {
        return LogType.SETINT
    }

    SetIntRecord(final int txNumber, final Block block,
                 final int offset, final int val) {
        this.txNumber = txNumber
        this.block = block
        this.offset = offset
        this.val = val
    }

    SetIntRecord(final BasicLogRecord rec) {
        this.txNumber = rec.nextInt()
        this.block = new Block(rec.nextString(), rec.nextInt())
        this.offset = rec.nextInt()
        this.val = rec.nextInt()
    }

    int write(final LogManager logManager) {
        return logManager.append(type.id, txNumber, block.fileName, block.number, offset, val)
    }
        
    void undo(final BufferManager bufferManager, final int txNumber) {
        bufferManager.withBuffer(block) { Buffer buffer ->
            buffer.setInt(offset, val, txNumber, -1)
        }
    }
}

@CompileStatic
@ToString(includePackage=false, includeNames=true)
class CheckpointRecord implements LogRecord {
    LogType getType() {
        return LogType.CHECKPOINT
    }

    int getTxNumber() {
        return -1
    }
    
    CheckpointRecord() {}

    CheckpointRecord(final BasicLogRecord rec) {}

    int write(final LogManager logManager) {
        return logManager.append(type.id)
    }
        
    void undo(final BufferManager bufferManager, final int txNumber) {}
}

@CompileStatic
@ToString(includePackage=false, includeNames=true)
abstract class TransactionOnlyRecord implements LogRecord {
    final int txNumber

    TransactionOnlyRecord(final int txNumber) {
        this.txNumber = txNumber
    }

    TransactionOnlyRecord(final BasicLogRecord rec){
        this(rec.nextInt())
    }

    int write(final LogManager logManager) {
        return logManager.append(type.id, txNumber)
    }
    
    void undo(final BufferManager bufferManager, final int txNumber) {}
}

@CompileStatic
@InheritConstructors
@ToString(includePackage=false, includeNames=true)
class StartRecord extends TransactionOnlyRecord {
    LogType getType() {
        return LogType.START
    }
}

@CompileStatic
@InheritConstructors
@ToString(includePackage=false, includeNames=true)
class CommitRecord extends TransactionOnlyRecord {
    LogType getType() {
        return LogType.COMMIT
    }
}

@CompileStatic
@InheritConstructors
@ToString(includePackage=false, includeNames=true)
class RollbackRecord extends TransactionOnlyRecord {
    LogType getType() {
        return LogType.ROLLBACK
    }
}
