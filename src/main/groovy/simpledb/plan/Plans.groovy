package simpledb.plan

import groovy.transform.CompileStatic
import simpledb.query.*
import simpledb.record.*
import simpledb.tx.Transaction
import simpledb.metadata.MetadataManager

@CompileStatic
class TablePlan implements Plan {
    @Delegate final StatisticsInfo stats
    final Schema schema
    private final Transaction tx
    private final TableInfo tableInfo

    TablePlan(final String tableName, final Transaction tx, final MetadataManager md) {
        this.tx = tx
        this.tableInfo = md.tableInfo(tableName, tx)
        this.stats = md.statisticsInfo(tableName, tx)
        this.schema = tableInfo.schema
    }

    Scan open() {
        return new TableScan(tableInfo, tx)
    }
}

@CompileStatic
class SelectPlan implements Plan {
    private final Plan parent
    private final Predicate predicate

    SelectPlan(final Plan parent, final Predicate predicate) {
        this.parent = parent
        this.predicate = predicate
    }

    Scan open() {
        return new SelectScan(parent.open(), predicate)
    }

    Schema getSchema() {
        return parent.schema
    }

    int getNumberBlocks() {
        return parent.numberBlocks
    }

    int getNumberRecords() {
        return (int) (parent.numberRecords / predicate.reductionFactor(parent))
    }

    int distinctValues(final String fieldName) {
        if(predicate.equatesWithConstant(fieldName) != null) {
            return 1
        }
        else {
            return Math.min(parent.distinctValues(fieldName), numberRecords)
        }
    }
}

@CompileStatic
class ProjectPlan implements Plan {
    @Delegate private final Plan parent
    final Schema schema

    ProjectPlan(final Plan parent, final Collection<String> fieldNames) {
        this.parent = parent
        List<Field> tmp = []
        fieldNames.each { String fieldName -> tmp << parent.schema.field(fieldName) }
        this.schema = Schema.fromFields(tmp)
    }

    Scan open() {
        return new ProjectScan(parent.open(), schema.fieldNames)
    }
}

@CompileStatic
class ProductPlan implements Plan {
    private final Plan first
    private final Plan second
    final Schema schema

    ProductPlan(final Plan first, final Plan second) {
        this.first = first
        this.second = second
        this.schema = Schema.fromSchemas([first.schema, second.schema])
    }

    Scan open() {
        return new ProductScan(first.open(), second.open())
    }

    int getNumberBlocks() {
        return first.numberBlocks + (first.numberBlocks * second.numberBlocks)
    }

    int getNumberRecords() {
        return first.numberRecords + second.numberRecords
    }

    int distinctValues(final String fieldName) {
        return (first.schema.hasField(fieldName) ?
                first.distinctValues(fieldName) :
                second.distinctValues(fieldName))
    }
}
