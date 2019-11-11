package simpledb.record

import groovy.transform.*
import java.sql.Types
import simpledb.file.Page

@CompileStatic
@ToString(includePackage=false, includeNames=true)
@EqualsAndHashCode
class Field {
    final String name
    final int type
    final int length

    public Field(final String name, final int type, final int length) {
        this.name = name
        this.type = type
        this.length = length
    }

    static Field newInt(final String name) {
        return new Field(name, Types.INTEGER, 0)
    }

    static Field newString(final String name, final int length) {
        return new Field(name, Types.VARCHAR, length)
    }
}

@CompileStatic
class Schema {
    private final Map<String,Field> _fields;

    private Schema(final Collection<Field> list) {
        Map<String,Field> tmp = [:]
        list.each { Field field -> tmp[field.name] = field }
        _fields = tmp.asImmutable()
    }
    
    static Schema fromSchemas(final Collection<Schema> list) {
        return new Schema(list.collect { Schema s -> s.fields }.flatten() as Collection<Field>)
    }

    static Schema fromFields(final Collection<Field> list) {
        return new Schema(list)
    }
    
    Field field(final String name) {
        return _fields[name]
    }

    Set<String> getFieldNames() {
        return _fields.keySet()
    }

    Collection<Field> getFields() {
        return _fields.values()
    }

    boolean hasField(final String fieldName) {
        return _fields.containsKey(fieldName)
    }
}

@CompileStatic
class TableInfo {
    final String tableName
    final Schema schema
    final Map<String,Integer> offsets
    final int recordLength

    TableInfo(final String tableName, final Schema schema) {
        this.tableName = tableName
        this.schema = schema

        int pos = 0
        Map<String,Integer> tmp = [:]
        schema.fields.each { Field field ->
            tmp[field.name] = pos;
            if(field.type == Types.INTEGER) {
                pos += 4
            }
            else if(field.type == Types.VARCHAR) {
                pos += Page.varcharLength(field.length)
            }
            else {
                throw new IllegalArgumentException()
            }
        }

        this.offsets = tmp.asImmutable()
        this.recordLength = pos
    }

    TableInfo(final String tableName, final Schema schema,
              final Map<String,Integer> offsets, final int recordLength) {
        this.tableName = tableName
        this.schema = schema
        this.offsets = offsets.asImmutable()
        this.recordLength = recordLength
    }

    String getFileName() {
        return "${tableName}.tbl"
    }

    int offset(final fieldName) {
        final Integer off = offsets.get(fieldName)
        if(off == null) {
            throw new UnknownFieldException("${fieldName} is not found")
        }

        return off.intValue()
    }
}

@CompileStatic
@ToString(includePackage=false, includeNames=true)
@EqualsAndHashCode
class StatisticsInfo {
    final int numberBlocks
    final int numberRecords

    StatisticsInfo(final int numberBlocks, final int numberRecords) {
        this.numberBlocks = numberBlocks
        this.numberRecords = numberRecords
    }

    int distinctValues(final String fieldName) {
        return 1 + (int) (numberRecords / 3)
    }
}
