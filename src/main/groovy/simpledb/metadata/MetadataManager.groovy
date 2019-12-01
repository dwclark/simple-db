package simpledb.metadata

import groovy.transform.CompileStatic
import simpledb.index.IndexInfo
import simpledb.index.IndexType
import simpledb.record.*
import simpledb.tx.Transaction

@CompileStatic
class MetadataManager {
    final TableManager tableManager
    final ViewManager viewManager
    final StatisticsManager statisticsManager
    final IndexManager indexManager
    
    MetadataManager(final boolean isNew, final Transaction tx) {
        tableManager = new TableManager(isNew, tx)
        viewManager = new ViewManager(isNew, tableManager, tx)
        statisticsManager = new StatisticsManager(tableManager, tx)
        indexManager = new IndexManager(isNew, tableManager, statisticsManager, tx)
    }

    void createTable(final String tableName, final Schema schema, final Transaction tx) {
        tableManager.createTable tableName, schema, tx
    }

    TableInfo tableInfo(final String tableName, final Transaction tx) {
        return tableManager.tableInfo(tableName, tx)
    }

    void createView(final String viewName, final String viewDefinition, final Transaction tx){
        viewManager.createView viewName, viewDefinition, tx
    }

    String viewDefinition(final String viewName, final Transaction tx) {
        return viewManager.viewDefinition(viewName, tx)
    }

    StatisticsInfo statisticsInfo(final String tableName, final Transaction tx) {
        return statisticsManager.info(tableName, tx)
    }
    
    void createIndex(final IndexType indexType, final String indexName, final String tableName,
                     final String fieldName, final Transaction tx) {
        indexManager.createIndex indexType, indexName, tableName, fieldName, tx
    }

    Map<String,IndexInfo> indexInfo(final String tableName, final Transaction tx) {
        return indexManager.indexInfo(tableName, tx)
    }
}
