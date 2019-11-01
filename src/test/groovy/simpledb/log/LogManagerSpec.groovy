package simpledb.log

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.Page

class LogManagerSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def 'test simple writes'() {
        setup:
        def builder = new Config.Builder(databaseName: 'test_simple_writes',
                                         databaseDirectory: tempFolder.root.path,
                                         logName: 'theLog.log')
        def server = new Server(builder.config())
        def logManager = server.logManager
        int lsn = logManager.with {
            append("a", "b")
            append("c", "d")
            append("e", "f")
        }
        
        logManager.flush(lsn)
        def iter = logManager.iterator()
        def first = iter.next()
        def second = iter.next()
        def third = iter.next()

        expect:
        first.nextString() == 'e'
        first.nextString() == 'f'
        second.nextString() == 'c'
        second.nextString() == 'd'
        third.nextString() == 'a'
        third.nextString() == 'b'
        !iter.hasNext()
    }

    def 'test file size'() {
        setup:
        def builder = new Config.Builder(databaseName: 'test_file_size',
                                         databaseDirectory: tempFolder.root.path,
                                         logName: 'theLog.log', pageSize: 400)
        def server = new Server(builder.config())
        def logManager = server.logManager
        def strings = (0..<10).collect { RandomStringUtils.randomAlphabetic(100) }
        strings.each { str -> logManager.append(str) }
        def iter = logManager.iterator()
        def file = new File(server.fileManager.directory, 'theLog.log')
        
        expect:
        strings.reverse().every { str -> str == iter.next().nextString() }
        file.exists()
        file.length() == server.config.pageSize * 4
    }

    def 'test ints and strings'() {
        def builder = new Config.Builder(databaseName: 'test_file_size',
                                         databaseDirectory: tempFolder.root.path,
                                         logName: 'theLog.log')
        def server = new Server(builder.config())
        def logManager = server.logManager
        def entries = (0..<100).collect { [ RandomUtils.nextInt(), RandomStringUtils.randomAlphabetic(50) ] }
        entries.each { e -> logManager.append(e as Object[]) }
        def iter = logManager.iterator();

        expect:
        entries.reverse().every { list ->
            def record = iter.next()
            list[0] == record.nextInt() && list[1] == record.nextString()
        }
    }
}
