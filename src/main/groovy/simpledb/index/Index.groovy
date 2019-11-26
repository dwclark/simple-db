package simpledb.index

import simpledb.query.Constant
import simpledb.record.RID
import simpledb.record.Schema
import simpledb.tx.Transaction

interface Index {
    public static final String DATAVAL = 'dataval'
    public static final String BLOCK = 'block'
    public static final String ID = 'id'
    
    void beforeFirst(Constant key)
    boolean next()
    RID getDataRid()
    void insert(Constant val, RID rid)
    void delete(Constant val, RID rid)
    void close()
}

interface IndexFactory {
    Index create(String indexName, Schema leafSchema, Transaction tx)
}
