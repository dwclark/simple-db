package simpledb.parse

import groovy.transform.CompileStatic
import simpledb.query.*
import simpledb.record.Schema
import simpledb.record.Field

@CompileStatic
class Parser {
    static final char COMMA = ',' as char
    private Lexer lex

    Parser(final String s) {
        lex = new Lexer(s)
    }

    String field() { lex.eatId() }

    Constant constant() {
        if(lex.matchStringConstant()) {
            return new StringConstant(lex.eatStringConstant())
        }
        else {
            return new IntConstant(lex.eatIntConstant())
        }
    }

    Expression expression() {
        if(lex.matchId()) {
            return new FieldNameExpression(field())
        }
        else {
            return new ConstantExpression(constant())
        }
    }

    Term term() {
        Expression lhs = expression()
        lex.eatDelimiter('=')
        Expression rhs = expression()
        return new Term(lhs, rhs)
    }

    Predicate predicate() {
        Predicate pred = new Predicate([term()])
        if(lex.matchKeyword('and')) {
            lex.eatKeyword('and')
            return pred.conjoinWith(predicate())
        }
        else {
            return pred
        }
    }

    QueryData query() {
        lex.eatKeyword('select')
        List<String> fields = selectList()
        lex.eatKeyword('from')
        List<String> tables = tableList()
        Predicate pred = Predicate.EMPTY
        if(lex.matchKeyword('where')) {
            lex.eatKeyword('where')
            pred = predicate()
        }

        return new QueryData(fields: fields, tables: tables, predicate: pred)
    }

    Object updateCommand() {
        if(lex.matchKeyword('insert')) {
            return insert()
        }
        else if(lex.matchKeyword('delete')) {
            return delete()
        }
        else if(lex.matchKeyword('update')) {
            return modify()
        }
        else if(lex.matchKeyword('create')) {
            return create()
        }
        else {
            throw new BadSyntaxException()
        }
    }

    DeleteData delete() {
        lex.with {
            Predicate pred = Predicate.EMPTY
            final String tableName = eatKeyword('delete').eatKeyword('from').eatId()
            
            if(matchKeyword('where')) {
                eatKeyword('where')
                pred = predicate()
            }
            
            return new DeleteData(tableName, pred)
        }
    }

    InsertData insert() {
        lex.with {
            final String tableName = lex.eatKeyword('insert').eatKeyword('into').eatId()
            lex.eatDelimiter('(')
            final List<String> fields = fieldList()
            eatDelimiter(')').eatKeyword('values').eatDelimiter('(')
            List<Constant> values = constList()
            lex.eatDelimiter(')')
            return new InsertData(tableName, fields, values)
        }
    }

    ModifyData modify() {
        String tableName = lex.eatKeyword('update').eatId()
        lex.eatKeyword('set')
        String fieldName = field()
        lex.eatDelimiter('=')
        Expression newVal = expression()
        Predicate pred = Predicate.EMPTY
        if(lex.matchKeyword('where')) {
            lex.eatKeyword('where')
            pred = predicate()
        }

        return new ModifyData(tableName, fieldName, newVal, pred)
    }

    CreateTableData createTable() {
        final String tableName = lex.eatKeyword('table').eatId()
        lex.eatDelimiter('(')
        final Schema schema = Schema.fromFields(fieldDefinitions())
        lex.eatDelimiter(')')
        return new CreateTableData(tableName, schema)
    }

    CreateViewData createView() {
        String viewName = lex.eatKeyword('view').eatId()
        lex.eatKeyword('as')
        QueryData qd = query()
        return new CreateViewData(viewName, qd)
    }

    CreateIndexData createIndex() {
        String indexName = lex.eatKeyword('index').eatId()
        String tableName = lex.eatKeyword('on').eatId()
        lex.eatDelimiter('(')
        String fieldName = field()
        lex.eatDelimiter(')')
        return new CreateIndexData(indexName, tableName, fieldName)
    }

    Object create() {
        lex.eatKeyword('create')
        if(lex.matchKeyword('table')) {
            return createTable()
        }
        else if(lex.matchKeyword('view')) {
            return createView()
        }
        else if(lex.matchKeyword('index')) {
            return createIndex()
        }
        else {
            throw new BadSyntaxException()
        }
    }

    private List<String> selectList() {
        return csv([], this.&field)
    }

    private List<String> tableList() {
        return csv([], this.&eatId)
    }

    private List<String> fieldList() {
        return csv([], this.&field)
    }

    private List<Constant> constList() {
        return csv([], this.&constant)
    }

    private <T> List<T> csv(final List<T> soFar, final Closure<T> closure) {
        soFar.add(closure())
        if(lex.matchDelimiter(COMMA)) {
            lex.eatDelimiter(COMMA)
            return csv(soFar, closure)
        }

        return soFar
    }

    private List<Field> fieldDefinitions() {
        return csv([], this.&fieldDefinition)
    }

    private Field fieldDefinition() {
        String fieldName = field()
        if(lex.matchKeyword("int")) {
            lex.eatKeyword("int")
            return Field.newInt(fieldName)
        }
        else if(lex.eatKeyword('varchar')) {
            int strLength = lex.eatKeyword('varchar').eatDelimiter('(').eatIntConstant()
            lex.eatDelimiter(')')
            return Field.newString(fieldName, strLength)
        }
        else {
            throw new BadSyntaxException()
        }
    }
}
