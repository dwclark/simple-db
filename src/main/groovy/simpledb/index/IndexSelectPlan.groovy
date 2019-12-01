package simpledb.index

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import simpledb.plan.Plan
import simpledb.query.*
import simpledb.record.Schema
import simpledb.tx.Transaction

@CompileStatic @TupleConstructor
class IndexSelectPlan implements Plan {
    final Plan plan
    @Delegate final IndexInfo ii
    final Constant val
    
    Scan open() {
        return new IndexSelectScan(ii.open(), val, (TableScan) plan.open())
    }

    int getBlocksAccessed() {
        return ii.blocksAccessed + recordsOutput
    }

    Schema getSchema() {
        return plan.schema
    }
}
