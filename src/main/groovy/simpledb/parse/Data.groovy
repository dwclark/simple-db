package simpledb.parse

import groovy.transform.CompileStatic
import groovy.transform.Immutable
import simpledb.index.IndexType
import simpledb.query.Constant
import simpledb.query.Expression
import simpledb.query.Predicate
import simpledb.record.Schema

@CompileStatic @Immutable
class CreateIndexData {
    IndexType indexType
    String indexName, tableName, fieldName
}

@CompileStatic @Immutable
class QueryData {
    List<String> fields, tables
    Predicate predicate

    @Override String toString() {
        String result = "select ${fields.join(', ')} from ${tables.join(', ')} "
        return result + (predicate ? " where ${predicate}" : "") 
    }
}

@CompileStatic @Immutable
class InsertData {
    String tableName
    List<String> fields
    List<Constant> values
}

@CompileStatic @Immutable
class DeleteData {
    String tableName
    Predicate predicate
}

@CompileStatic @Immutable
class ModifyData {
    String tableName, fieldName
    Expression newValue
    Predicate predicate
}

@CompileStatic @Immutable
class CreateTableData {
    String tableName
    Schema schema
}

@CompileStatic @Immutable
class CreateViewData {
    String viewName
    QueryData queryData
}
