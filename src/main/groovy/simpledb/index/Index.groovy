package simpledb.index

import simpledb.record.RID
import simpledb.query.Constant

interface Index {
    void beforeFirst(Constant key)
    boolean next()
    RID getDataRid()
    void insert(Constant val, RID rid)
    void delete(Constant val, RID rid)
    void close()
}
