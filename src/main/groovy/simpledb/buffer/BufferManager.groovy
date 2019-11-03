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
    final FileManager fileManager
    final LogManager logManager
    final long maxWait
    
    private final Lock lock
    private final Condition condition
    
    private LRUMap<Block,Buffer> pool
    private int _available
    
    BufferManager(final FileManager fileManager, final LogManager logManager,
                  final int maxBuffers, final long maxWait) {
        this.fileManager = fileManager
        this.logManager = logManager
        this.maxWait = maxWait
        
        pool = new LRUMap(maxBuffers, maxBuffers, 0.75f)
        _available = maxBuffers
        for(int i = 0; i < maxBuffers; ++i) {
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

    void withBuffer(final Block block, final Closure<Buffer> closure) {
        final Buffer buffer = pin(block)
        try {
            closure.call(buffer)
        }
        finally {
            unpin(buffer)
        }
    }

    Buffer pin(final Block block) {
        return withWait(this.&_pin.curry(block))
    }

    Buffer pin(final String fileName, final PageFormatter formatter){
        return withWait(this.&_pinNew.curry(fileName, formatter))
    }

    void unpin(final Buffer buffer) {
        withLock {
            _unpin(buffer)
            if(!buffer.pinned) {
                condition.signalAll()
            }
        }
    }

    int getAvailable() {
        int ret
        withLock { ret = _available }
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
                condition.await(maxWait, TimeUnit.MILLISECONDS)
                buffer = closure.call()
            }
            
            if(buffer == null) {
                throw new BufferAbortException()
            }
        }

        return buffer
    }

    private boolean tooLong(final long start) {
        final long now = System.currentTimeMillis()
        return now - start > maxWait
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
            --_available
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
        --_available
        buffer.pin()
        return buffer
    }

    private void _unpin(final Buffer buffer) {
        buffer.unpin()
        if(!buffer.pinned) {
            ++_available
        }
    }

    private Buffer unpinnedBuffer() {
        final Iterator<Map.Entry<Block,Buffer>> iter = pool.entrySet().iterator()
        while(iter.hasNext()) {
            Map.Entry<Block,Buffer> entry = iter.next()
            if(!entry.value.pinned) {
                Buffer buffer = entry.value
                iter.remove()
                return buffer
            }
        }

        return null
    }
}
