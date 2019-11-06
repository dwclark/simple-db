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

    private Field(final String name, final int type, final int length) {
        this.name = name
        this.type = type
        this.length = length
    }

    Field newInt(final String name) {
        return new Field(name, Types.INTEGER, 0)
    }

    Field newString(final String name, final int length) {
        return new Field(name, Types.VARCHAR, length)
    }
}

@CompileStatic
class Schema {
    private final Map<String,Field> _fields = [:]

    void add(final Field field) {
        _fields[field.name] = field
    }

    void addAll(final Schema s) {
        _fields.putAll(s._fields)
    }

    Field field(final String name) {
        return _fields[name]
    }

    Collection<String> getFieldNames() {
        return _fields.keySet()
    }

    Collection<Field> getFields() {
        return _fields.values()
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

        this.offsets = Collections.unmodifiableMap(tmp)
        this.recordLength = pos
    }

    TableInfo(final String tableName, final Schema schema,
              final Map<String,Integer> offsets, final int recordLength) {
        this.tableName = tableName
        this.schema = schema
        this.offsets = offsets
        this.recordLength = recordLength
    }

    String getFileName() {
        return "${tableName}.tbl"
    }

    int offset(final fieldName) {
        return offsets.get(fieldName)
    }
}