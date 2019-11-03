package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

public enum LogType {
    CHECKPOINT(0, { BasicLogRecord blr -> new CheckpointRecord(blr) }),
        START(1, { BasicLogRecord blr -> new StartRecord(blr) }),
        COMMIT(2, { BasicLogRecord blr -> new CommitRecord(blr) }),
        ROLLBACK(3, { BasicLogRecord blr -> new RollbackRecord(blr) }),
        SETINT(4, { BasicLogRecord blr -> new SetIntRecord(blr) }),
        SETSTRING(5, { BasicLogRecord blr -> new SetStringRecord(blr) })
    
    private LogType(final int id, final Closure<LogRecord> factory) {
        this.id = id;
        this.factory = factory
    }
    
    final int id;
    final Closure<LogRecord> factory

    public static final Map<Integer,LogType> idMap = populateIdMap();

    private static Map<Integer,LogType> populateIdMap() {
        final Map<Integer,LogType> ret = new LinkedHashMap<>();
        for(LogType logType : values()) {
            ret.put(logType.id, logType);
        }

        return Collections.unmodifiableMap(ret);
    }
}
