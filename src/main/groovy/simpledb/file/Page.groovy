package simpledb.file

import groovy.transform.*
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@CompileStatic
class Page {
    static final int SIZE = 400
    static final int INT_SIZE = 4
    static final Charset CHARSET = StandardCharsets.UTF_8
    static final int MAX_CHAR_BYTE = (int) CHARSET.newEncoder().maxBytesPerChar()
    
    static final int strSize(final int n) {
        return INT_SIZE + (n * MAX_CHAR_BYTE)
    }

    final FileManager fileManager
    
    protected Page(final FileManager fileManager) {
        this.fileManager = fileManager
    }

    private ByteBuffer contents = ByteBuffer.allocateDirect(SIZE)

    @Synchronized
    void read(final Block block) {
        fileManager.read(block, contents)
    }

    @Synchronized
    void write(final Block block) {
        fileManager.write(block, contents)
    }

    @Synchronized
    Block append(final String fileName) {
        return fileManager.append(fileName, contents)
    }

    @Synchronized
    int getInt(final int offset) {
        return contents.getInt(offset)
    }

    @Synchronized
    void setInt(final int offset, final int val) {
        contents.putInt(offset, val)
    }

    @Synchronized
    String getString(final int offset) {
        contents.position(offset)
        final int len = contents.getInt()
        byte[] bytes = new byte[len]
        contents.get(bytes)
        return new String(bytes, CHARSET)
    }

    void setString(final int offset, final String val) {
        byte[] bytes = val.getBytes(CHARSET)
        contents.position(offset)
        contents.putInt(bytes.length)
        contents.put(bytes)
    }
}
