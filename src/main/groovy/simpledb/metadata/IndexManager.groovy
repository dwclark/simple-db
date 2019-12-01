package simpledb.metadata

import groovy.transform.CompileStatic
import simpledb.index.IndexInfo
import simpledb.index.IndexType
import simpledb.record.RecordFile
import simpledb.record.Schema
import simpledb.record.TableInfo
import simpledb.tx.Transaction
import static simpledb.record.Field.*

@CompileStatic
class IndexManager {
    static final String INDEX_TABLE = 'idxcat'
    static final String INDEX_TYPE = 'indextype'
    static final String INDEX_NAME = 'indexname'
    static final String TABLE_NAME = 'tablename'
    static final String FIELD_NAME = 'fieldname'

    final TableManager tableManager
    final StatisticsManager statisticsManager
    
    IndexManager(final boolean isNew, final TableManager tableManager,
                 final StatisticsManager statisticsManager, final Transaction tx) {
        this.tableManager = tableManager
        this.statisticsManager = statisticsManager
        
        if(isNew) {
            Schema schema = Schema.fromFields(newInt(INDEX_TYPE),
                                              newString(INDEX_NAME, TableManager.MAX_NAME),
                                              newString(TABLE_NAME, TableManager.MAX_NAME),
                                              newString(FIELD_NAME, TableManager.MAX_NAME))
            tableManager.createTable(INDEX_TABLE, schema, tx)
        }
    }

    private TableInfo tableInfo(final Transaction tx) {
        return tableManager.tableInfo(INDEX_TABLE, tx)
    }

    void createIndex(final IndexType indexType, final String indexName, final String tableName,
                     final String fieldName, final Transaction tx) {
        new RecordFile(tableInfo(tx), tx).with {
            insert()
            setInt INDEX_TYPE, indexType.id
            setString INDEX_NAME, indexName
            setString TABLE_NAME, tableName
            setString FIELD_NAME, fieldName
            close()
        }
    }

    Map<String,IndexInfo> indexInfo(final String tableName, final Transaction tx) {
        Map<String,IndexInfo> ret = [:]
        new RecordFile(tableInfo(tx), tx).with {
            while(next()) {
                if(getString(TABLE_NAME) == tableName) {
                    IndexType indexType = IndexType.fromId(getInt(INDEX_TYPE))
                    String indexName = getString(INDEX_NAME)
                    String fieldName = getString(FIELD_NAME)
                    ret[fieldName] = new IndexInfo(indexName, fieldName, indexType, tx, tableInfo,
                                                   statisticsManager.info(tableName, tx))
                }
            }

            close()
        }

        return ret
    }
}
