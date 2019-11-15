package simpledb.parse

import java.sql.Types
import spock.lang.*
import simpledb.query.*

class ParserSpec extends Specification {

    def 'test query parse'() {
        setup:
        def select = 'select foo, bar from blah'

        when:
        QueryData qd = new Parser(select).query()

        then:
        qd.tables.size() == 1
        qd.tables[0] == 'blah'
        qd.fields.size() == 2
        qd.fields == [ 'foo', 'bar' ]
        qd.predicate.is(Predicate.EMPTY)        
    }

    def 'test query parse with predicate'() {
        setup:
        def select = "select foo from blah, poo where bazinga = 10 and floopy = 'bar bar'"

        when:
        QueryData qd = new Parser(select).query()

        then:
        qd.tables.size() == 2
        qd.tables == [ 'blah', 'poo' ]
        qd.fields.size() == 1
        qd.fields == [ 'foo' ]
        qd.predicate.terms.size() == 2

        when:
        def term1 = qd.predicate.terms[0]
        def term2 = qd.predicate.terms[1]

        then:
        term1.lhs.fieldName
        term1.lhs.asFieldName() == 'bazinga'
        term1.rhs.constant
        term1.rhs.asConstant() instanceof IntConstant
        term1.rhs.asConstant().val == 10
        
        term2.lhs.fieldName
        term2.lhs.asFieldName() == 'floopy'
        term2.rhs.constant
        term2.rhs.asConstant() instanceof StringConstant
        term2.rhs.asConstant().val == 'bar bar'
    }

    def 'test insert parse'() {
        setup:
        def insert = "insert into students (boo, foo) values (10, 'me')"

        when:
        InsertData id = new Parser(insert).updateCommand()

        then:
        id.tableName == 'students'
        id.fields == [ 'boo', 'foo' ]
        id.values == [ new IntConstant(10), new StringConstant('me') ]
    }

    def 'test update parse'() {
        setup:
        def update = "update invoices set date = '10/10/2000' where number = 100"

        when:
        ModifyData md = new Parser(update).updateCommand()

        then:
        md.tableName == 'invoices'
        md.fieldName == 'date'
        md.newValue.asConstant() == new StringConstant('10/10/2000')
        md.predicate.terms[0].lhs.asFieldName() == 'number'
        md.predicate.terms[0].rhs.asConstant() == new IntConstant(100)
    }

    def 'test delete parse'() {
        setup:
        def delete = "delete from invoices where date = '10/10/2000'"

        when:
        DeleteData dd = new Parser(delete).updateCommand()

        then:
        dd.tableName == 'invoices'
        dd.predicate.terms[0].lhs.asFieldName() == 'date'
        dd.predicate.terms[0].rhs.asConstant() == new StringConstant('10/10/2000')
    }

    def 'test create index'() {
        setup:
        def index = "create index dumbindex on students (name)"

        when:
        CreateIndexData cid = new Parser(index).updateCommand()

        then:
        cid.indexName == 'dumbindex'
        cid.tableName == 'students'
        cid.fieldName == 'name'
    }

    def 'test create view'() {
        setup:
        def view = 'create view failing as select name from students where grade = 80'

        when:
        CreateViewData cvd = new Parser(view).updateCommand()

        then:
        cvd.viewName == 'failing'
    }

    def 'test create table'(){
        setup:
        def sql = '''create table students (
id int,
firstname varchar(10),
lastname varchar(10)
)'''
        when:
        CreateTableData c = new Parser(sql).updateCommand()

        then:
        c.tableName == 'students'
        c.schema.field('id').type == Types.INTEGER
        c.schema.field('firstname').type == Types.VARCHAR
        c.schema.field('lastname').type == Types.VARCHAR
    }
}

