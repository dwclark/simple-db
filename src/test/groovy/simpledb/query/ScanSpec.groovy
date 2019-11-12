package simpledb.query

import java.sql.Types
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import simpledb.file.*
import simpledb.server.*
import simpledb.tx.Transaction
import simpledb.record.*
import spock.lang.*

class ScanSpec extends Specification {

    @Rule TemporaryFolder tempFolder

    def studentSchema
    def studentInfo

    def studentList = [['scooby', 'doo', 1],
                       ['scrappy', 'doo', 2],
                       ['fred', 'van halen', 3],
                       ['wilma', 'flintstone', 4]]

    def setup() {
        studentSchema = Schema.fromFields([Field.newString('first', 10),
                                           Field.newString('last', 10),
                                           Field.newInt('id')])
        
        studentInfo = new TableInfo("students", studentSchema)
    }

    private void insertAll(final server) {
        def transaction = server.newTransaction()
        def scan = new TableScan(studentInfo, transaction)
        studentList.each { list ->
            scan.with {
                insert()
                setString('first', list[0])
                setString('last', list[1])
                setInt('id', list[2])
            }
        }
        
        transaction.commit()
    }

    def 'test basic table scan'() {
        setup:
        def builder = new Config.Builder(databaseName: 'basic_table_scan',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        insertAll(server)

        when:
        def transaction = server.newTransaction()
        def scan = new TableScan(studentInfo, transaction)
        def toTest = []
        scan.with {
            while(next()) {
                toTest.add([getString('first'), getString('last'), getInt('id')])
            }
        }

        then:
        toTest == studentList
    }

    def 'test basic select scan'() {
        setup:
        def builder = new Config.Builder(databaseName: 'basic_select_scan',
                                         databaseDirectory: tempFolder.root.path)
        def server = new Server(builder.config())
        insertAll(server)

        when:
        def lhs = new FieldNameExpression('last')
        def rhs = new ConstantExpression(new StringConstant('doo'))
        def term = new Term(lhs, rhs)
        def pred = new Predicate(term)
        def transaction = server.newTransaction()
        def tableScan = new TableScan(studentInfo, transaction)
        def selectScan = new SelectScan(tableScan, pred)
        def toTest = []
        selectScan.with {
            while(next()) {
                println "${getString('first')}"
                println "${getString('last')}"
                println "${getInt('id')}"
                toTest.add([getString('first'), getString('last'), getInt('id')])
            }
        }

        then:
        toTest.size() == 2
    }
}
