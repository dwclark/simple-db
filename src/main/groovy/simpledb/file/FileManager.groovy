package simpledb.file

import java.nio.ByteBuffer
import groovy.transform.*
import java.util.concurrent.ConcurrentHashMap
import java.nio.channels.FileChannel
import java.io.RandomAccessFile
import java.util.function.BiFunction

@CompileStatic
class FileManager {
    final File directory
    final boolean isNew
    final int pageSize
    private ConcurrentHashMap<String,FileChannel> openFiles = new ConcurrentHashMap<>();

    FileManager(final String base, final String name, final int pageSize) {
        this.pageSize = pageSize
        directory = new File(base, name)
        isNew = !directory.exists()

        if(isNew && !directory.mkdir()) {
            throw new RuntimeException("Could not create db directory ${directory.name}")
        }

        directory.eachFile { File file ->
            if(file.name.startsWith("temp")) {
                file.delete()
            }
        }
    }

    void read(final Block block, final ByteBuffer buf) {
        openFiles.compute(block.fileName) { String fileName, FileChannel existing ->
            FileChannel channel = (existing == null) ? createChannel(fileName) : existing
            buf.clear()
            channel.read(buf, block.number * buf.capacity())
            return channel
        } as BiFunction
    }

    void write(final Block block, final ByteBuffer buf) {
        openFiles.compute(block.fileName) { String fileName, FileChannel existing ->
            return _write((existing == null) ? createChannel(fileName) : existing, block, buf)
        } as BiFunction
    }

    Block append(final String fileName, final ByteBuffer buf) {
        Block block = null
        openFiles.compute(fileName) { String arg, FileChannel existing ->
            FileChannel channel = (existing == null) ? createChannel(fileName) : existing
            block = new Block(fileName, _size(channel))
            return _write(channel, block, buf)
        } as BiFunction

        return block
    }

    int size(final String fileName) {
        return _size(openFiles.computeIfAbsent(fileName, this.&createChannel))
    }

    private FileChannel _write(final FileChannel channel, final Block block, final ByteBuffer buf) {
        buf.rewind()
        channel.write(buf, block.number * buf.capacity())
        return channel
    }

    private int _size(final FileChannel channel) {
        return (int) (channel.size() / pageSize)
    }

    private FileChannel createChannel(final String fileName) {
        final File file = new File(directory, fileName)
        return new RandomAccessFile(file, "rws").channel
    }

    Page newPage() {
        return new Page(this)
    }
}
