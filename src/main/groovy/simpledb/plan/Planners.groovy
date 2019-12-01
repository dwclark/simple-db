package simpledb.plan

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import simpledb.parse.*
import simpledb.query.*
import simpledb.tx.Transaction
import simpledb.metadata.MetadataManager

@CompileStatic @TupleConstructor
class BasicQueryPlanner implements QueryPlanner {
    final MetadataManager metadataManager
    
    Plan plan(final QueryData data, final Transaction tx) {
        List<Plan> plans = []
        data.tables.each { String tableName ->
            String viewDef = metadataManager.viewDefinition(tableName, tx)
            if(viewDef) {
                plans << plan(new Parser(viewDef).query(), tx)
            }
            else {
                plans << new TablePlan(tableName, tx, metadataManager)
            }
        }

        Plan ret = plans.remove(0)
        plans.each { Plan tmp -> ret = new ProductPlan(ret, tmp) }
        ret = new SelectPlan(ret, data.predicate)
        ret = new ProjectPlan(ret, data.fields)
        return ret
    }
}

@CompileStatic @TupleConstructor
class BasicUpdatePlanner implements UpdatePlanner {
    final MetadataManager metadataManager
    
    int execute(final InsertData data, final Transaction tx) {
        Iterator<Constant> valIter = data.values.iterator()
        Iterator<String> fieldIter = data.fields.iterator()
        new TablePlan(data.tableName, tx, metadataManager).open().with {
            insert()
            while(valIter.hasNext()) {
                setVal(fieldIter.next(), valIter.next())
            }
            
            close()
        }

        return 1
    }
    
    int execute(final DeleteData data, final Transaction tx) {
        Plan tp = new TablePlan(data.tableName, tx, metadataManager)
        Plan sp = new SelectPlan(tp, data.predicate)
        int count = 0
        sp.open().with {
            while(next()) {
                delete()
                ++count
            }

            close()
        }

        return count
    }
    
    int execute(final ModifyData data, final Transaction tx) {
        Plan tp = new TablePlan(data.tableName, tx, metadataManager)
        int count = 0
        new SelectPlan(tp, data.predicate).open().with {
            while(next()) {
                Constant val = data.newValue.evaluate(it)
                setVal(data.fieldName, val)
                ++count
            }

            close()
        }

        return count
    }
    
    int execute(final CreateTableData data, final Transaction tx) {
        metadataManager.createTable(data.tableName, data.schema, tx)
        return 0
    }
    
    int execute(final CreateViewData data, final Transaction tx) {
        metadataManager.createView(data.viewName, data.queryData.toString(), tx)
        return 0
    }
    
    int execute(final CreateIndexData data, final Transaction tx) {
        metadataManager.createIndex(data.indexType, data.indexName, data.tableName,
                                    data.fieldName, tx)
        return 0
    }
}
