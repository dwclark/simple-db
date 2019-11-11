package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public interface Plan {
    Scan open();
    int getNumberBlocks();
    int getNumberRecords();
    int distinctValues(String fieldName);
    Schema getSchema();
}
