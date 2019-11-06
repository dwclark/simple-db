package simpledb.record

import groovy.transform.*
import simpledb.buffer.PageFormatter
import simpledb.file.Page
import java.sql.Types

@CompileStatic
class RecordFormatter implements PageFormatter {
    final TableInfo tableInfo
    final int pageSize
    
    RecordFormatter(final TableInfo tableInfo, final int pageSize) {
        this.tableInfo = tableInfo
        this.pageSize = pageSize
    }

    void format(final Page page) {
        final int recordSize = tableInfo.recordLength + Page.INT_SIZE
        for(int pos = 0; pos + recordSize < pageSize; pos += recordSize) {
            page.setInt(pos, RecordPage.EMPTY)
            defaultRecord(page, pos + Page.INT_SIZE)
        }
    }

    private void defaultRecord(final Page page, final int pos) {
        tableInfo.schema.fields.each { Field field ->
            final int offset = tableInfo.offset(field.name)
            if(field.type == Types.INTEGER) {
                page.setInt(pos + offset, 0)
            }
            else if(field.type == Types.VARCHAR) {
                page.setString(pos + offset, "")
            }
        }
    }
}
