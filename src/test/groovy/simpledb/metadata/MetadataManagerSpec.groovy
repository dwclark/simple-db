package simpledb.metadata

import spock.lang.*
import org.junit.rules.TemporaryFolder
import org.junit.Rule
import simpledb.server.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import simpledb.file.*
import simpledb.record.*

class MetadataManagerSpec extends Specification {

    @Rule TemporaryFolder tempFolder
    def schema = Schema.fromFields([Field.newInt('id'), Field.newString('name', 20)])
    
    def createStudents(def server) {
        def metadata = server.metadataManager
        def tx = server.newTransaction()
        metadata.createTable 'students', schema, tx
        tx.commit()
    }
    
    def 'test create table'() {
        setup:
        def builder = new Config.Builder(databaseName: 'create_table', databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def metadata = server.metadataManager

        when:
        createStudents server
        def tx = server.newTransaction()
        def tableInfo = metadata.tableInfo('students', tx)
        tx.commit()
        
        then:
        tableInfo.tableName == 'students'
        tableInfo.schema.fieldNames.size() == 2
    }

    def 'test create view'() {
        setup:
        def builder = new Config.Builder(databaseName: 'create_view', databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def metadata = server.metadataManager
        def view = 'select id from students'
        
        when:
        createStudents server
        def tx = server.newTransaction()
        def tableInfo = metadata.createView('student_ids', view, tx)
        tx.commit()

        tx = server.newTransaction()
        def viewDefinition = metadata.viewDefinition('student_ids', tx)
        tx.commit()
        
        then:
        view == viewDefinition
    }

}
