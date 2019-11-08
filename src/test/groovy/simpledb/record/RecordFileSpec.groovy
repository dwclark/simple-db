package simpledb.record

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*
import java.sql.Types

class RecordFileSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def schema
    def tableInfo

    def setup() {
        schema = new Schema()
        schema.add(Field.newInt("A"))
        tableInfo = new TableInfo("junk", schema)
    }

    def 'test basic record File'() {
        setup:
        def builder = new Config.Builder(databaseName: 'record_file', maxBuffers: 10,
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def fileManager = server.fileManager
        def transaction = server.newTransaction()
        def rf = new RecordFile(tableInfo, transaction)
        def random = new Random()

        when:
        10_000.times {
            rf.insert()
            rf.setInt("A", random.nextInt(200))
        }

        rf.beforeFirst()
        while(rf.next()) {
            if(rf.getInt("A") < 100) {
                rf.delete()
            }
        }

        def list = []
        rf.beforeFirst()
        while(rf.next()) {
            list << rf.getInt("A")
        }
        
        then:
        list.every { num -> num >= 100 }
        list.size > 10
        
        cleanup:
        rf.close()
        transaction.commit()
    }
}
