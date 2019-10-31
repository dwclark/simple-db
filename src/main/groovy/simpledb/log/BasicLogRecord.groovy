package simpledb.log

import simpledb.file.Page
import groovy.transform.CompileStatic
import groovy.transform.CompileStatic

@CompileStatic
class BasicLogRecord {
    final Page page
    private int pos

    BasicLogRecord(final Page page, final int pos) {
        this.page = page
        this.pos = pos
    }

    int nextInt() {
        int ret = page.getInt(pos)
        pos += Page.INT_SIZE
        return ret
    }

    String nextString() {
        int augment = page.getStringLength(pos)
        String ret = page.getString(pos)
        pos += augment
        return ret
    }
}
