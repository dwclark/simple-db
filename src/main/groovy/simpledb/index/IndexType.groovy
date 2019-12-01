package simpledb.index

import groovy.transform.CompileStatic
import java.util.function.IntBinaryOperator
import simpledb.index.btree.BPTreeIndex
import simpledb.index.hash.HashIndex

@CompileStatic
enum IndexType {
    BTREE(1, 'btree', BPTreeIndex.COST, BPTreeIndex.FACTORY),
        HASH(2, 'hash', HashIndex.COST, HashIndex.FACTORY);

    
    private IndexType(final int id, final String description,
                      final IntBinaryOperator cost, final IndexFactory factory) {
        this.id = id
        this.description = description
        this.cost = cost
        this.factory = factory
    }

    final int id
    final String description
    final IntBinaryOperator cost
    final IndexFactory factory

    static IndexType fromId(final int id) {
        if(id == BTREE.id) {
            return BTREE
        }
        else if(id == HASH.id) {
            return HASH
        }
        else {
            throw new IllegalArgumentException()
        }
    }

    static IndexType fromDescription(final String description) {
        final String lookFor = description.toLowerCase()
        if(lookFor == BTREE.description) {
            return BTREE
        }
        else if(lookFor == HASH.description) {
            return HASH
        }
        else {
            throw new IllegalArgumentException()
        }
    }
}
