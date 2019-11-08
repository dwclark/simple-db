package simpledb.record

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*
import java.sql.Types

class RecordPageSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def schema
    def tableInfo

    def setup() {
        schema = new Schema()
        schema.add(Field.newInt("id"))
        schema.add(Field.newString("name", 10))
        tableInfo = new TableInfo("students", schema)
    }

    def 'test basic record page'() {
        setup:
        def builder = new Config.Builder(databaseName: 'record_page', maxBuffers: 10,
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def fileManager = server.fileManager
        def transaction = server.newTransaction()
        def formatter = new RecordFormatter(tableInfo, fileManager.pageSize)

        when:
        def block = transaction.append(tableInfo.fileName, formatter)
        def rp = new RecordPage(block, tableInfo, transaction)
        rp.insert()
        rp.setInt("id", 100)
        rp.setString("name", "david")
        rp.insert()
        rp.setInt("id", 101)
        rp.setString("name", "scooby")
        rp.close()
        
        transaction.commit()

        transaction = server.newTransaction()
        rp = new RecordPage(block, tableInfo, transaction)
        
        then:
        rp.next()
        rp.getInt("id") == 100
        rp.getString("name") == "david"
        rp.next()
        rp.getInt("id") == 101
        rp.getString("name") == "scooby"
        !rp.next()

        // cleanup:
        // rp.close()
        // transaction.commit()
    }
}
