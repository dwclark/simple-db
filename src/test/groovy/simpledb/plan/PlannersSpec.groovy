package simpledb.plan

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

    def 'test basic planner operations'() {
        setup:
        def builder = new Config.Builder(databaseName: 'basic_planner',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        def newTable = '''create table students (
id int,
firstname varchar(10),
lastname varchar(10)
)'''
        def inserts = ["insert into students (id, firstname, lastname) values (1, 'David', 'Clark')",
                       "insert into students (id, firstname, lastname) values (2, 'Bob', 'Smith')",
                       "insert into students (id, firstname, lastname) values (3, 'Aristotle', 'Smith')"]
        when:
        def tx = server.newTransaction()
        def createTableRows = server.planner.execute(newTable, tx)
        def insertCounts = []
        inserts.each { insertCounts << server.planner.execute(it, tx) }
        tx.commit()
        
        then:
        createTableRows == 0
        insertCounts == [ 1, 1, 1]

        when:
        tx = server.newTransaction()
        def updateBob = "update students set lastname = 'Jones' where id = 2"
        def updateCount = server.planner.execute(updateBob, tx)
        tx.commit()

        then:
        updateCount == 1

        when:
        tx = server.newTransaction()
        def deleteAristotle = "delete from students where lastname = 'Smith'"
        def deleteCount = server.planner.execute(deleteAristotle, tx)
        tx.commit()

        then:
        deleteCount == 1

        when:
        tx = server.newTransaction()
        def selectAll = "select id, firstname, lastname from students"
        def rows = []
        server.planner.query(selectAll, tx).open().with {
            while(next()) {
                rows << [ getInt('id'), getString('firstname'), getString('lastname') ]
            }
            
            close()
        }

        then:
        rows.size() == 2
        rows[0] == [ 1, 'David', 'Clark' ]
        rows[1] == [ 2, 'Bob', 'Jones' ]
        
    }
}
