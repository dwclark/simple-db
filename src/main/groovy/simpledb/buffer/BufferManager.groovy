package simpledb.buffer

import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import simpledb.file.*
import simpledb.log.LogManager
import org.apache.commons.collections4.map.LRUMap
import java.util.concurrent.locks.*
import java.util.concurrent.TimeUnit

@CompileStatic
class BufferManager {
    private static final long TOO_LONG = 10_000L

    final FileManager fileManager
    final LogManager logManager

    private final Lock lock
    private final Condition condition
    
    private LRUMap<Block,Buffer> pool
    private int available
    
    BufferManager(final FileManager fileManager, final LogManager logManager,
                  final int max) {
        this.fileManager = fileManager
        this.logManager = logManager
        
        pool = new LRUMap(max, max, 0.75f)
        available = max
        for(int i = 0; i < max; ++i) {
            Buffer buf = new Buffer(fileManager, logManager)
            pool.put(buf.block, buf)
        }

        lock = new ReentrantLock()
        condition = lock.newCondition()
    }

    void flushAll(final int txNum) {
        withLock {
            pool.each { Block block, Buffer buffer ->
                if(buffer.isModifiedBy(txNum)) {
                    buffer.flush()
                }
            }
        }
    }

    public Buffer pin(final Block block) {
        return withWait(this.&_pin.curry(block))
    }

    public Buffer pin(final String fileName, final PageFormatter formatter){
        return withWait(this.&_pinNew.curry(fileName, formatter))
    }

    public void unpin(final Buffer buffer) {
        withLock {
            _unpin(buffer)
            if(!buffer.pinned) {
                notifyAll()
            }
        }
    }

    public int available() {
        int ret
        withLock { ret = available }
        return ret
    }

    private void withLock(final Closure closure) {
        try {
            lock.lock()
            closure()
        }
        catch(InterruptedException e) {
            throw new BufferAbortException(e)
        }
        finally {
            lock.unlock()
        }
    }

    private Buffer withWait(final Closure<Buffer> closure) {
        Buffer buffer
        
        withLock {
            long timestamp = System.currentTimeMillis()
            buffer = closure.call()
            while(buffer == null && !tooLong(timestamp)) {
                condition.await(TOO_LONG, TimeUnit.MILLISECONDS)
                buffer = closure.call()
            }
            
            if(buffer == null) {
                throw new BufferAbortException()
            }

            return buffer
        }
    }

    private boolean tooLong(final long start) {
        final long now = System.currentTimeMillis()
        return now - start > TOO_LONG
    }

    private Buffer _pin(final Block block) {
        Buffer buffer = pool.get(block)
        if(buffer == null) {
            buffer = unpinnedBuffer()
            if(buffer == null) {
                return null
            }

            buffer.assign(block)
            pool.put(block, buffer)
        }

        if(!buffer.pinned) {
            --available
        }

        buffer.pin()
        return buffer
    }

    private Buffer _pinNew(final String fileName, final PageFormatter formatter) {
        Buffer buffer = unpinnedBuffer()
        if(buffer == null) {
            return null
        }

        buffer.assign(fileName, formatter)
        pool.put(buffer.block, buffer)
        --available
        buffer.pin()
        return buffer
    }

    void _unpin(final Buffer buffer) {
        buffer.unpin()
        if(!buffer.pinned) {
            --available
        }
    }

    private Buffer unpinnedBuffer() {
        final Iterator<Map.Entry<Block,Buffer>> iter = pool.entrySet().iterator()
        while(iter.hasNext()) {
            Map.Entry<Block,Buffer> entry = iter.next()
            if(!entry.value.pinned) {
                iter.remove()
                return entry.value
            }
        }

        return null
    }
}
