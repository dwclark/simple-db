package simpledb.buffer

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*

class BufferManagerSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def 'test pin and unpins'() {
        setup:
        def builder = new Config.Builder(databaseName: 'pins_unpins', maxBuffers: 10,
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def bufferManager = server.bufferManager
        def txId = 1

        when:
        def all = []
        (0..4).each { num ->
            def block = new Block('stringtable', num)
            def str = RandomStringUtils.randomAlphabetic(400)
            def buffer = bufferManager.pin(block)
            buffer.setString(0, str, txId, 1)
            all.add([ block, str, buffer ])
        }

        then:
        bufferManager.available == 5

        when:
        bufferManager.flushAll txId
        all.each { list -> bufferManager.unpin(list[2]) }
        Collections.shuffle(all)
        
        then:
        bufferManager.available == 10
        all.every { list ->
            def block = list[0]
            def buffer = bufferManager.pin(block)
            def str = buffer.getString(0)
            bufferManager.unpin(buffer)
            list[1] == str
        }            
    }

    def 'test wait for buffer'() {
        setup:
        def builder = new Config.Builder(databaseName: 'wait_for_buffer', maxBuffers: 1,
                                         databaseDirectory: tempFolder.root.path,
                                         useMetadata: false)
        def server = new Server(builder.config())
        def bufferManager = server.bufferManager
        def txId = 1

        when:
        def block = new Block('foo', 0)
        def buffer = bufferManager.pin(block)
        buffer.setString(0, 'scooby doo', txId, 1)

        then:
        bufferManager.available == 0

        when:
        def thread = Thread.start {
            def myBlock = new Block('foo', 1)
            def myBuffer = bufferManager.pin(myBlock)
            myBuffer.setString(0, 'scooby doo again', txId, 1)
            bufferManager.unpin(myBuffer)
        }

        sleep(1_000)
        bufferManager.unpin(buffer)
        thread.join()
        bufferManager.flushAll(txId)
        def dbFolder = new File(tempFolder.root, server.config.databaseName)
        def file = new File(dbFolder, 'foo')
        
        then:
        file.exists()
        file.length() == server.config.pageSize * 2
    }
}
