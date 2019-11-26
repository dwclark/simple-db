package simpledb.metadata

import groovy.transform.CompileStatic
import simpledb.record.Schema
import simpledb.record.TableInfo
import simpledb.tx.Transaction
import static simpledb.record.Field.*

@CompileStatic
class IndexManager {
    static final String TABLE = 'idxcat'
    static final String 
    final TableInfo tableInfo

    IndexManager(final boolean isNew, final TableManager tableManager, final Transaction tx) {
        if(isNew) {
            Schema schema = Schema.fromFields(newInt('indextype'),
                                              newString('indexname', TableManager.MAX_NAME),
                                              newString('tablename', TableManager.MAX_NAME),
                                              newString('fieldname', TableManager.MAX_NAME))
            tableManager.createTable('idxcat', schema, tx)
        }
        
        tableInfo = tableManager.tableInfo('idxcat', tx)
    }

    //TODO: start here
    void createIndex(final int indexType, final String indexName, final String tableName,
                     final String fieldName, final Transaction tx) {
        
    }
}
