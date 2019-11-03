package simpledb.tx.recovery

import groovy.transform.CompileStatic
import simpledb.log.*

@CompileStatic
class LogRecordIterator implements Iterator<LogRecord> {
    private Iterator<BasicLogRecord> iter;

    LogRecordIterator(final LogManager logManager) {
        iter = logManager.iterator()
    }

    boolean hasNext() {
        return iter.hasNext()
    }

    void remove() {
        throw new UnsupportedOperationException()
    }

    LogRecord next() {
        BasicLogRecord rec = iter.next()
        int id = rec.nextInt()
        return LogType.idMap[id].factory.call(rec)
    }
}
