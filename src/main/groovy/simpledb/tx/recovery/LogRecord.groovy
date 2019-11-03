package simpledb.tx.recovery

import simpledb.buffer.BufferManager
import simpledb.log.LogManager

interface LogRecord {
    int write(LogManager logManager)
    LogType getType()
    int getTxNumber()
    void undo(BufferManager bufferManager, int txNumber)
}
