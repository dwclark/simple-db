package simpledb.server

import groovy.transform.*
import simpledb.file.FileManager
import simpledb.log.LogManager

@CompileStatic
class Server {
    final Config config
    final FileManager fileManager
    final LogManager logManager

    Server(final Config config) {
        this.config = config
        this.fileManager = new FileManager(config.databaseDirectory, config.databaseName)
        this.logManager = new LogManager(config.logName, fileManager)
    }

    Server() {
        this(new Config.Builder().config())
    }
}
