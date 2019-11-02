package simpledb.tx.concurrency

import groovy.transform.CompileStatic
import simpledb.file.Block
import java.util.concurrent.locks.*
import java.util.concurrent.TimeUnit

@CompileStatic
class LockTable {
    private final long maxWait
    private final Lock lock
    private final Condition condition
    
    private Map<Block,Set<Integer>> sharedLocks
    private Map<Block,Integer> exclusiveLocks

    LockTable(final long maxWait) {
        this.maxWait = maxWait
        lock = new ReentrantLock()
        condition = lock.newCondition()
        sharedLocks = [:]
        exclusiveLocks = [:]
    }

    void sharedLock(final Block block, final int txId) {
        withWait(block, txId, this.&canShareLock, this.&grabShared) 
    }

    void exclusiveLock(final Block block, final int txId) {
        withWait(block, txId, this.&canExclusivelyLock, exclusiveLocks.&put)
    }

    void unlock(final Block block, final int txId) {
        withLock {
            exclusiveLocks.remove(block)
            final Set<Integer> txs = sharedLocks.get(block)
            if(txs != null) {
                txs.remove(txId)
            }
        }
    }

    private void withLock(final Closure closure) {
        try {
            lock.lock()
            closure()
        }
        catch(InterruptedException e) {
            throw new LockAbortException(e)
        }
        finally {
            lock.unlock()
        }
    }

    private void withWait(final Block block, final Integer txId,
                          final Closure<Boolean> doTest, final Closure doLock) {
        withLock {
            long timestamp = System.currentTimeMillis()
            while(doTest.call(block) && !tooLong(timestamp)) {
                condition.await(maxWait, TimeUnit.MILLISECONDS)
            }

            if(doTest.call(block)) {
                throw new LockAbortException()
            }

            doLock.call(block)
        }
    }

    private boolean canShareLock(final Block block, final Integer txId) {
        final Integer owner = exclusiveLocks.get(block)
        return (owner == null || owner == txId)
    }

    private void grabShared(final Block block, final Integer txId) {
        Set<Integer> txs = sharedLocks.get(block)
        if(txs == null) {
            txs = new HashSet<>()
            sharedLocks.put(block, txs)
        }

        txs.add(txId)
    }

    private boolean canExclusivelyLock(final Block block, final Integer txId) {
        final Set<Integer> txs = sharedLocks.get(block)
        return (txs == null ||
                txs.size() == 0 ||
                txs.size() == 1 && txs.iterator().next() == txId)
    }

    private boolean tooLong(final long start) {
        final long now = System.currentTimeMillis()
        return now - start > maxWait
    }

}
