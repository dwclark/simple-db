package simpledb.parse

import java.io.StreamTokenizer
import groovy.transform.CompileStatic

@CompileStatic
class Lexer {
    static final Set<String> keywords = (['select', 'from', 'where',
                                          'and', 'insert', 'into', 'values', 'int',
                                          'varchar', 'update', 'set', 'delete', 'on',
                                          'create', 'table', 'view', 'as', 'index'] as Set).asImmutable()
    static final int PERIOD = ('.' as char) as int
    
    private final StreamTokenizer tok

    Lexer(String s) {
        this(new StringReader(s))
    }

    Lexer(Reader r) {
        tok = new StreamTokenizer(r)
        tok.ordinaryChar PERIOD
        tok.lowerCaseMode true
        tok.nextToken()
    }

    private <T> T guard(final boolean doIt, final Closure<T> closure) {
        if(doIt) {
            T val = closure()
            tok.nextToken()
            return val
        }
        else {
            throw new BadSyntaxException()
        }
    }
    
    boolean matchDelimiter(final char c) { c == (char) tok.ttype }
    boolean matchIntConstant() { StreamTokenizer.TT_NUMBER == tok.ttype }
    boolean matchStringConstant() { "'" as char == (char) tok.ttype }
    boolean matchKeyword(final String w) { StreamTokenizer.TT_WORD == tok.ttype && tok.sval == w }
    boolean matchId() { StreamTokenizer.TT_WORD == tok.ttype && !keywords.contains(tok.sval) }

    Lexer eatDelimiter(final char c) {
        guard(matchDelimiter(c)) { this }
    }

    Lexer eatDelimiter(final String s) {
        return eatDelimiter(s as char)
    }

    int eatIntConstant() {
        guard(matchIntConstant()) { return tok.nval }
    }

    String eatStringConstant() {
        guard(matchStringConstant()) { return tok.sval }
    }

    Lexer eatKeyword(final String w) {
        guard(matchKeyword(w)) { this }
    }

    String eatId() {
        guard(matchId()) { return tok.sval }
    }
}
