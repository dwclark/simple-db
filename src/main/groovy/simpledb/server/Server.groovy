package simpledb.server

import groovy.transform.*
import simpledb.file.FileManager

@CompileStatic
class Server {
    final String databaseName
    final Config config
    final FileManager fileManager

    Server(final Config config, final String databaseName) {
        this.databaseName = databaseName
        this.config = config
        this.fileManager = new FileManager(config.databaseDirectory, databaseName)
    }

    Server() {
        this(new Config.Builder().config(), databaseName)
    }
}
