package simpledb.metadata

import groovy.transform.CompileStatic
import simpledb.record.*
import simpledb.tx.Transaction

@CompileStatic
class TableManager {
    static final int MAX_NAME = 16
    static final String TABLE_CATALOG = 'tableCatalog'
    static final String FIELD_CATALOG = 'fieldCatalog'
    static final String TABLE_NAME = 'tableName'
    static final String RECORD_LENGTH = 'recordLength'
    static final String FIELD_NAME = 'fieldName'
    static final String TYPE = 'type'
    static final String LENGTH = 'length'
    static final String OFFSET = 'offset'
    
    private TableInfo tables
    private TableInfo fields

    TableManager(final boolean isNew, final Transaction tx) {
        Schema tableSchema = Schema.fromFields([Field.newString(TABLE_NAME, MAX_NAME),
                                                Field.newInt(RECORD_LENGTH)])
        tables = new TableInfo(TABLE_CATALOG, tableSchema)
        
        Schema fieldSchema = Schema.fromFields([Field.newString(TABLE_NAME, MAX_NAME),
                                                Field.newString(FIELD_NAME, MAX_NAME),
                                                Field.newInt(TYPE),
                                                Field.newInt(LENGTH),
                                                Field.newInt(OFFSET)])

        fields = new TableInfo(FIELD_CATALOG, fieldSchema)

        if(isNew) {
            createTable TABLE_CATALOG, tableSchema, tx
            createTable FIELD_CATALOG, fieldSchema, tx
        }
    }

    void createTable(final String tableName, final Schema schema, final Transaction tx) {
        final TableInfo tableInfo = new TableInfo(tableName, schema)
        new RecordFile(tables, tx).with {
            insert()
            setString(TABLE_NAME, tableName)
            setInt(RECORD_LENGTH, tableInfo.recordLength)
            close()
        }
        
        new RecordFile(fields, tx).with {
            schema.fields.each { Field field ->
                insert()
                setString(TABLE_NAME, tableName)
                setString(FIELD_NAME, field.name)
                setInt(TYPE, field.type)
                setInt(LENGTH, field.length)
                setInt(OFFSET, tableInfo.offset(field.name))
            }

            close()
        }
    }

    TableInfo tableInfo(final String tableName, final Transaction tx) {
        int recordLength = -1
        new RecordFile(tables, tx).with {
            while(next()) {
                if(getString(TABLE_NAME) == tableName) {
                    recordLength = getInt(RECORD_LENGTH)
                    break;
                }
            }

            close()
        }

        List<Field> tmp = []
        Map<String,Integer> offsets = new LinkedHashMap<>()
        new RecordFile(fields, tx).with {
            while(next()) {
                if(getString(TABLE_NAME) == tableName) {
                    Field field = new Field(getString(FIELD_NAME), getInt(TYPE), getInt(LENGTH))
                    offsets[field.name] = getInt(OFFSET)
                    tmp << field
                }
            }

            close()
        }

        return new TableInfo(tableName, Schema.fromFields(tmp), offsets, recordLength)
    }
}
