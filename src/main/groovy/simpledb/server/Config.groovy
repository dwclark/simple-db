package simpledb.server

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames=true, includePackage=false)
class Config {
    final String databaseName
    final String databaseDirectory
    final String logName
    
    private Config(final Builder builder) {
        databaseName = builder.databaseName
        databaseDirectory = builder.databaseDirectory
        logName = builder.logName
    }
    
    static class Builder {

        String databaseName = System.properties['simple.db.name'] ?: 'simpledb'
        String databaseDirectory = System.properties['simple.db.dir']
        String logName = System.properties['simple.db.log.name']
        
        Config config() {
            return new Config(this)
        }
    }
}
