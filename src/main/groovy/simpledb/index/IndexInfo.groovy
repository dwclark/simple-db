package simpledb.index

import groovy.transform.CompileStatic
import java.util.function.IntBinaryOperator
import simpledb.record.*
import simpledb.tx.Transaction
import static simpledb.index.Index.*

@CompileStatic
class IndexInfo {
    final Transaction tx
    final TableInfo tableInfo
    final StatisticsInfo statisticsInfo
    final String indexName
    final String fieldName
    final IndexType type

    IndexInfo(final String indexName, final String fieldName, final int indexType,
              final Transaction tx, final TableInfo tableInfo, final StatisticsInfo statisticsInfo) {
        this.indexName = indexName
        this.fieldName = fieldName
        this.type = IndexType.fromId(indexType)
        this.tx = tx
        this.tableInfo = tableInfo
        this.statisticsInfo = statisticsInfo
    }

    Index open() {
        return type.factory.create(indexName, schema, tx)
    }
    
    int getNumberBlocks() {
        int rpb = (int) (tx.bufferManager.fileManager.pageSize / tableInfo.recordLength)
        int numberBlocks = (int) (statisticsInfo.numberRecords / rpb)
        return type.cost.applyAsInt(numberBlocks, rpb)
    }

    int getNumberRecords() {
        return (int) (statisticsInfo.numberRecords / statisticsInfo.distinctValues(fieldName))
    }

    int distinctValues(final String fname) {
        if(fname == fieldName) {
            return 1
        }
        else {
            return Math.min(statisticsInfo.distinctValues(fname), numberRecords)
        }
    }

    Schema getSchema() {
        return Schema.fromFields(Field.newInt(BLOCK), Field.newInt(ID),
                                 tableInfo.schema.field(fieldName).changeName(DATAVAL))
    }
}
