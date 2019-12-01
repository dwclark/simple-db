package simpledb.index

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*
import java.sql.Types

class IndexesSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def 'test basic index creation'() {
        setup:
        def builder = new Config.Builder(databaseName: 'basic_index_creation',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def newTable = '''create table students (
id int,
firstname varchar(10),
lastname varchar(10)
)'''
        def createHashIndex = 'create index ididx on students(id) using hash'
        def createBtreeIndex1 = 'create index fnameidx on students(firstname) using btree'
        def createBtreeIndex2 = 'create index lnameidx on students(lastname)'
        
        when:
        def tx = server.newTransaction()
        def createTableRows = server.planner.execute(newTable, tx)
        def createHashIndexRows = server.planner.execute(createHashIndex, tx)
        def createBtreeIndex1Rows = server.planner.execute(createBtreeIndex1, tx)
        def createBtreeIndex2Rows = server.planner.execute(createBtreeIndex2, tx)
        tx.commit()

        then:
        createTableRows == 0
        createBtreeIndex1Rows == 0
        createBtreeIndex2Rows == 0
    }
}
