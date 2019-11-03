package simpledb.tx

import groovy.transform.CompileStatic
import simpledb.buffer.*
import simpledb.file.Block

@CompileStatic
class BufferList {
    private final Map<Block,Buffer> buffers
    private final List<Block> pins
    private final BufferManager bufferManager

    BufferList(final BufferManager bufferManager) {
        this.buffers = [:]
        this.pins = new ArrayList<>()
        this.bufferManager = bufferManager
    }

    Buffer get(final Block block) {
        return buffers[block]
    }

    void pin(final Block block) {
        final Buffer buffer = bufferManager.pin(block)
        buffers[block] = buffer
        pins.add(block)
    }

    Block pin(final String fileName, final PageFormatter formatter) {
        final Buffer buffer = bufferManager.pin(fileName, formatter)
        final Block block = buffer.block
        buffers[block] = buffer
        pins.add(block)
        return block
    }

    void unpin(final Block block) {
        final Buffer buffer = buffers[block]
        bufferManager.unpin(buffer)
        pins.remove(block)
        if(!pins.contains(block)) {
            buffers.remove(block)
        }
    }

    void unpin() {
        pins.each { Block block -> bufferManager.unpin(buffers[block]) }
        buffers.clear()
        pins.clear()
    }
}
