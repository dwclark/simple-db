package simpledb.server

import groovy.transform.*
import simpledb.file.FileManager
import simpledb.log.LogManager
import simpledb.buffer.BufferManager
import simpledb.tx.concurrency.LockTable

@CompileStatic
class Server {
    final Config config
    final FileManager fileManager
    final LogManager logManager
    final BufferManager bufferManager
    final LockTable lockTable
    
    Server(final Config config) {
        this.config = config
        this.fileManager = new FileManager(config.databaseDirectory, config.databaseName, config.pageSize)
        this.logManager = new LogManager(config.logName, fileManager)
        this.bufferManager = new BufferManager(fileManager, logManager, config.maxBuffers, config.maxBufferWait)
        this.lockTable = new LockTable(config.maxLockWait)
    }

    Server() {
        this(new Config.Builder().config())
    }
}
