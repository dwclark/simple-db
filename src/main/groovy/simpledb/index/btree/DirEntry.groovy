package simpledb.index.btree

import simpledb.query.Constant
import groovy.transform.Immutable
import groovy.transform.CompileStatic

@CompileStatic @Immutable
class DirEntry {
    Constant dataVal
    int blockNumber
}
