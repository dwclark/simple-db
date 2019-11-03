package simpledb.tx.concurrency

import groovy.transform.CompileStatic
import simpledb.file.Block

@CompileStatic
class ConcurrencyManager {

    final LockTable lockTable
    final int txNumber
    private final Map<Block,LockType> locks
    
    ConcurrencyManager(final int txNumber, final LockTable lockTable) {
        this.lockTable = lockTable
        this.txNumber = txNumber
        locks = [:]
    }

    void sharedLock(final Block block) {
        if(!locks.containsKey(block)) {
            lockTable.sharedLock(block, txNumber)
            locks.put(block, LockType.S)
        }
    }

    void exclusiveLock(final Block block) {
        if(!LockType.X.equals(locks.get(block))) {
            lockTable.exclusiveLock(block, txNumber)
            locks.put(block, LockType.X)
        }
    }

    void release() {
        locks.keySet().each { Block block -> lockTable.unlock(block, txNumber) }
        locks.clear()
    }
}
