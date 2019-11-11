package simpledb.metadata

import groovy.transform.CompileStatic
import simpledb.tx.Transaction
import simpledb.record.*

@CompileStatic
class ViewManager {

    static final int MAX_DEFINITION = 100
    static final String VIEW_NAME = 'viewName'
    static final String VIEW_DEFINITION = 'viewDefinition'
    static final String VIEW_CATALOG = 'viewCatalog'
    
    final TableManager tableManager

    ViewManager(final boolean isNew, final TableManager tableManager, final Transaction tx) {
        this.tableManager = tableManager
        if(isNew) {
            Schema schema = Schema.fromFields([Field.newString(VIEW_NAME, TableManager.MAX_NAME),
                                               Field.newString(VIEW_DEFINITION, MAX_DEFINITION)])
            tableManager.createTable(VIEW_CATALOG, schema, tx)
        }
    }

    void createView(final String viewName, final String viewDefinition, final Transaction tx) {
        final TableInfo tableInfo = tableManager.tableInfo(VIEW_CATALOG, tx)
        new RecordFile(tableInfo, tx).with {
            insert()
            setString(VIEW_NAME, viewName)
            setString(VIEW_DEFINITION, viewDefinition)
            close()
        }
    }

    String viewDefinition(final String viewName, final Transaction tx) {
        String ret = null
        final TableInfo tableInfo = tableManager.tableInfo(VIEW_CATALOG, tx)
        new RecordFile(tableInfo, tx).with {
            while(next()) {
                if(getString(VIEW_NAME) == viewName) {
                    ret = getString(VIEW_DEFINITION)
                    break;
                }
            }

            close()
        }

        return ret
    }
}
