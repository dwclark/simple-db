package simpledb.query

import groovy.transform.CompileStatic
import simpledb.record.RID
import simpledb.record.RecordFile
import simpledb.record.Schema
import simpledb.record.TableInfo
import simpledb.tx.Transaction
import java.sql.Types

@CompileStatic
class TableScan implements Scan {
    @Delegate private final RecordFile recordFile
    private final Schema schema

    TableScan(final TableInfo ti, final Transaction tx) {
        this.recordFile = new RecordFile(ti, tx)
        this.schema = ti.schema
    }

    Constant getVal(final String fieldName) {
        final int type = schema.field(fieldName).type
        if(type == Types.INTEGER) {
            return new IntConstant(recordFile.getInt(fieldName))
        }
        else if(type == Types.VARCHAR) {
            return new StringConstant(recordFile.getString(fieldName))
        }
        else {
            throw new IllegalArgumentException("${fieldName} is of unknown type")
        }
    }

    void setVal(final String fieldName, final Constant c) {
        final int type = schema.field(fieldName).type
        if(type == Types.INTEGER) {
            IntConstant i = (IntConstant) c
            recordFile.setInt(fieldName, i.val)
        }
        else if(type == Types.VARCHAR) {
            StringConstant s = (StringConstant) c
            recordFile.setString(fieldName, s.val)
        }
        else {
            throw new IllegalArgumentException("${fieldName} is of unknown type")
        }
    }
}

@CompileStatic
class SelectScan implements Scan {
    @Delegate private final Scan scan
    private final Predicate predicate
    
    SelectScan(final Scan scan, final Predicate predicate) {
        this.scan = scan
        this.predicate = predicate
    }
    
    boolean next() {
        while(scan.next()) {
            if(predicate.test(scan)) {
                return true
            }
        }

        return false
    }
}

@CompileStatic
class ProjectScan implements Scan {
    @Delegate private final Scan scan
    private final Set<String> fieldNames

    ProjectScan(final Scan scan, final Set<String> fieldNames) {
        this.scan = scan
        this.fieldNames = fieldNames
    }

    boolean hasField(final String fieldName) {
        return fieldNames.contains(fieldName)
    }

    private String check(final String fieldName) {
        if(fieldNames.contains(fieldName)) {
            return fieldName
        }
        else {
            throw new IllegalArgumentException("${fieldName} not found")
        }
    }

    Constant getVal(final String fieldName) { scan.getVal(check(fieldName)) }
    void setVal(final String fieldName, final Constant c) { scan.setVal(check(fieldName), c) }
    int getInt(final String fieldName) { scan.getInt(check(fieldName)) }
    void setInt(final String fieldName, final int i) {scan.setInt(check(fieldName), i) }
    String getString(final String fieldName) { scan.getString(check(fieldName)) }
    void setString(final String fieldName, final String s) { scan.setString(check(fieldName), s) }
}

@CompileStatic
class ProductScan implements Scan {
    private final Scan first
    private final Scan second

    ProductScan(final Scan first, final Scan second) {
        this.first = first
        this.second = second
        first.next()
    }

    void beforeFirst() {
        first.beforeFirst()
        first.next()
        second.beforeFirst()
    }

    boolean next() {
        if(second.next()) {
            return true
        }
        else {
            second.beforeFirst()
            return second.next() && first.next()
        }
    }

    void close() {
        first.close()
        second.close()
    }

    Constant getVal(final String fieldName) {
        return first.hasField(fieldName) ? first.getVal(fieldName) : second.getVal(fieldName)
    }

    int getInt(final String fieldName) {
        return first.hasField(fieldName) ? first.getInt(fieldName) : second.getInt(fieldName)
    }

    String getString(final String fieldName) {
        return first.hasField(fieldName) ? first.getString(fieldName) : second.getString(fieldName)
    }

    boolean hasField(final String fieldName) {
        return first.hasField(fieldName) || second.hasField(fieldName)
    }
}
