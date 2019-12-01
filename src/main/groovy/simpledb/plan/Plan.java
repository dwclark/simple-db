package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public interface Plan {
    Scan open();
    int getBlocksAccessed();
    int getRecordsOutput();
    int distinctValues(String fieldName);
    Schema getSchema();
}
