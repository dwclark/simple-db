package simpledb.tx.concurrency

import groovy.transform.CompileStatic
import simpledb.file.Block

@CompileStatic
class ConcurrencyManager {

    final LockTable lockTable
    final int txId
    private final Map<Block,LockType> locks
    
    ConcurrencyManager(final LockTable lockTable, final int txId) {
        this.lockTable = lockTable
        this.txId = txId
        locks = [:]
    }

    void sharedLock(final Block block) {
        if(!locks.containsKey(block)) {
            lockTable.sharedLock(block, txId)
            locks.put(block, LockType.S)
        }
    }

    void exclusiveLock(final Block block) {
        if(!LockType.X.equals(locks.get(block))) {
            lockTable.exclusiveLock(block, txId)
            locks.put(block, LockType.X)
        }
    }

    void release() {
        locks.keySet().each { Block block -> lockTable.unlock(block, txId) }
        locks.clear()
    }
}
