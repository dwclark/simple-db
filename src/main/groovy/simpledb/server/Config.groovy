package simpledb.server

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames=true, includePackage=false)
class Config {
    final String databaseName
    final String databaseDirectory
    final String logName
    final int pageSize
    final int maxBuffers
    final long maxBufferWait
    
    private Config(final Builder builder) {
        databaseName = builder.databaseName
        databaseDirectory = builder.databaseDirectory
        logName = builder.logName
        pageSize = builder.pageSize
        maxBuffers = builder.maxBuffers
        maxBufferWait = builder.maxBufferWait
    }
    
    static class Builder {

        String databaseName = extractString('simple.db.name', 'simpledb')
        String databaseDirectory = extractString('simple.db.dir', '/tmp')
        String logName = extractString('simple.db.log.name', 'simpledb.log')
        int pageSize = extractInt('simple.db.page.size', 512)
        int maxBuffers = extractInt('simple.db.max.buffers', 512)
        long maxBufferWait = extractLong('simple.db.max.buffer.wait', 10_000L)
        
        Config config() {
            return new Config(this)
        }

        private String extractString(final String name, final String defaultValue) {
            if(System.properties.containsKey(name)) {
                return System.getProperty(name)
            }
            else {
                return defaultValue
            }
        }

        private int extractInt(final String name, final int defaultValue) {
            if(System.properties.containsKey(name)) {
                return System.getProperty(name).toInteger()
            }
            else {
                return defaultValue
            }
        }

        private int extractLong(final String name, final long defaultValue) {
            if(System.properties.containsKey(name)) {
                return System.getProperty(name).toLong()
            }
            else {
                return defaultValue
            }
        }
    }
}
