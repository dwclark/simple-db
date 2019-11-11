package simpledb.query;

import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;
import java.sql.Types;

public interface Scan {
    void beforeFirst();
    boolean next();
    void close();
    Constant getVal(String fieldName);
    int getInt(String fieldName);
    String getString(String fieldName);
    boolean hasField(String fieldName);

    default void setVal(String fieldName, Constant val) {
        throw new UnsupportedOperationException();
    }
    
    default void setInt(String fieldName, int val) {
        throw new UnsupportedOperationException();
    }
    
    default void setString(String fieldName, String val) {
        throw new UnsupportedOperationException();
    }
    
    default void insert() {
        throw new UnsupportedOperationException();
    }
    
    default void delete() {
        throw new UnsupportedOperationException();
    }
    
    default RID getCurrentRid() {
        throw new UnsupportedOperationException();
    }
    
    default void moveToRid(RID rid) {
        throw new UnsupportedOperationException();
    }
}
