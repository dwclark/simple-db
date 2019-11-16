package simpledb.plan

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import simpledb.parse.*
import simpledb.tx.Transaction

interface QueryPlanner {
    Plan plan(QueryData data, Transaction tx)
}

interface UpdatePlanner {
    int execute(InsertData data, Transaction tx)
    int execute(DeleteData data, Transaction tx)
    int execute(ModifyData data, Transaction tx)
    int execute(CreateTableData data, Transaction tx)
    int execute(CreateViewData data, Transaction tx)
    int execute(CreateIndexData data, Transaction tx)
}

@CompileStatic @TupleConstructor
class Planner {
    final QueryPlanner queryPlanner
    final UpdatePlanner updatePlanner

    Plan query(final String cmd, final Transaction tx) {
        return queryPlanner.plan(new Parser(cmd).query(), tx)
    }

    int execute(final String cmd, final Transaction tx) {
        final Object o = new Parser(cmd).updateCommand()
        switch(o.getClass()) {
            case InsertData: return updatePlanner.execute((InsertData) o, tx)
            case DeleteData: return updatePlanner.execute((DeleteData) o, tx)
            case ModifyData: return updatePlanner.execute((ModifyData) o, tx)
            case CreateTableData: return updatePlanner.execute((CreateTableData) o, tx)
            case CreateViewData: return updatePlanner.execute((CreateViewData) o, tx)
            case CreateIndexData: return updatePlanner.execute((CreateIndexData) o, tx)
            default: return 0
        }
    }
}
