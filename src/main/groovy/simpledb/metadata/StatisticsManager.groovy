package simpledb.metadata

import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import simpledb.record.*
import simpledb.tx.Transaction

@CompileStatic
class StatisticsManager {
    final TableManager tableManager
    private Map<String,StatisticsInfo> tableStatistics
    private int numberCalls

    StatisticsManager(final TableManager tableManager, final Transaction tx) {
        this.tableManager = tableManager
        refresh(tx)
    }

    @Synchronized
    StatisticsInfo info(final String tableName, final Transaction tx) {
        ++numberCalls
        if(numberCalls > 100) {
            refresh(tx)
        }

        StatisticsInfo ret = tableStatistics[tableName]
        if(ret == null) {
            final TableInfo tableInfo = tableManager.tableInfo(tableName, tx)
            ret = calculate(tableInfo, tx)
            tableStatistics[tableName] = ret
        }

        return ret
    }

    @Synchronized
    private void refresh(final Transaction tx) {
        tableStatistics = [:]
        numberCalls = 0
        final TableInfo tableInfo = tableManager.tableInfo(TableManager.TABLE_CATALOG, tx)
        new RecordFile(tableInfo, tx).with {
            while(next()) {
                final String tableName = getString(TableManager.TABLE_NAME)
                tableStatistics[tableName] = calculate(tableManager.tableInfo(tableName, tx), tx) 
            }

            close()
        }
    }

    @Synchronized
    private StatisticsInfo calculate(final TableInfo ti, final Transaction tx){
        int numberRecords = 0
        int numberBlocks = 0
        new RecordFile(ti, tx).with {
            while(next()) {
                ++numberRecords
                numberBlocks = currentRid.blockNumber + 1
            }

            close()
        }

        return new StatisticsInfo(numberBlocks, numberRecords)
    }
}
