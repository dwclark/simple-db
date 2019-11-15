package simpledb.parse

import groovy.transform.CompileStatic
import simpledb.query.*

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
        lex.eatDelimiter('=' as char)
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
        else {
            return create()
        }
    }

    private List<String> selectList() {
        List<String> ret = [ field() ]
        if(lex.matchDelimiter(COMMA)) {
            lex.eatDelimiter(COMMA)
            ret.addAll(selectList())
        }

        return ret
        
    }

    private List<String> tableList() {
        List<String> ret = [ lex.eatId() ]
        if(lex.matchDelimiter(COMMA)) {
            lex.eatDelimiter(COMMA)
            ret.addAll(tableList())
        }

        return ret
    }
}
