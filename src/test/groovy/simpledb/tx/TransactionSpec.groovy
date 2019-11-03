package simpledb.tx

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*

class TransactionSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def 'test simple commit'() {
        setup:
        def builder = new Config.Builder(databaseName: 'test_simple_commit',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def t1 = server.newTransaction()
        def id = 100
        def name = 'Scooby Doo'
        
        t1.with {
            def block = new Block('students', 0)
            def pos = 0
            pin(block)
            setInt(block, pos, id)
            pos += Page.INT_SIZE
            setString(block, pos, name)
            commit()
        }

        def t2 = server.newTransaction()
        def foundId, foundName
        t2.with {
            def block = new Block('students', 0)
            def pos = 0
            pin(block)
            foundId = getInt(block, pos)
            pos += Page.INT_SIZE
            foundName = getString(block, pos)
            commit()
        }

        expect:
        id == foundId
        name == foundName
    }
    
    def 'test simple rollback'() {
        setup:
        def builder = new Config.Builder(databaseName: 'test_simple_rollback',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def t1 = server.newTransaction()
        def id = 100
        def name = 'Scooby Doo'
        
        t1.with {
            def block = new Block('students', 0)
            def pos = 0
            pin(block)
            setInt(block, pos, id)
            pos += Page.INT_SIZE
            setString(block, pos, name)
            commit()
        }

        def t2 = server.newTransaction()
        t2.with {
            def block = new Block('students', 0)
            def pos = 0
            pin(block)
            setInt(block, pos, 200)
            pos += Page.INT_SIZE
            setString(block, pos, 'Shaggy')
            rollback()
        }

        def t3 = server.newTransaction()
        def foundId, foundName
        t3.with {
            def block = new Block('students', 0)
            def pos = 0
            pin(block)
            foundId = getInt(block, pos)
            pos += Page.INT_SIZE
            foundName = getString(block, pos)
            commit()
        }

        expect:
        id == foundId
        name == foundName
    }
}
