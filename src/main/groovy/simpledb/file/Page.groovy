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
    int setInt(final int offset, final int val) {
        contents.putInt(offset, val)
        return INT_SIZE
    }

    @Synchronized
    String getString(final int offset) {
        contents.position(offset)
        final int len = contents.getInt()
        byte[] bytes = new byte[len]
        contents.get(bytes)
        return new String(bytes, CHARSET)
    }

    @Synchronized
    int getStringLength(final int offset) {
        return INT_SIZE + contents.getInt(offset)
    }

    @Synchronized
    int setString(final int offset, final String val) {
        byte[] bytes = val.getBytes(CHARSET)
        contents.position(offset)
        contents.putInt(bytes.length)
        contents.put(bytes)
        return INT_SIZE + bytes.length
    }

    int setObject(final int offset, final Object o) {
        if(o instanceof String) {
            return setString(offset, (String) o)
        }
        else if(o instanceof Integer) {
            return setInt(offset, ((Integer) o).intValue())
        }
        else {
            throw new IllegalArgumentException("Can't serialize type ${o.getClass()}")
        }
    }

    static int length(final Object o) {
        if(o instanceof Integer) {
            return INT_SIZE
        }
        else if(o instanceof CharSequence) {
            return length((CharSequence) o)
        }
        else {
            throw new IllegalArgumentException("Can't determine serialized length of ${o.getClass()}")
        }
    }

    static int length(final CharSequence cs) {
        int count = 0
        final int len = cs.length()
        for(int i = 0; i < len; i++) {
            final char c = cs.charAt(i)
            if (c <= 0x7F) {
                count++
            }
            else if(c <= 0x7FF) {
                count += 2
            }
            else if(Character.isHighSurrogate(c)) {
                count += 4
                ++i
            }
            else {
                count += 3
            }
        }
        
        return count
    }
}
