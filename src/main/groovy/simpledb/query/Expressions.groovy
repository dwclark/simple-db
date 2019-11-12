package simpledb.query

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import simpledb.record.Schema

@CompileStatic @TupleConstructor
class ConstantExpression implements Expression {
    final Constant val;
    boolean isConstant() { true }
    boolean isFieldName() { false }
    Constant asConstant() { val }
    String asFieldName() { throw new ClassCastException() }
    Constant evaluate(final Scan s) { val }
    boolean appliesTo(final Schema schema) { true }
    @Override String toString() { val.toString() }
}

@CompileStatic @TupleConstructor
class FieldNameExpression implements Expression {
    final String fieldName
    boolean isConstant() { false }
    boolean isFieldName() { true }
    Constant asConstant() { throw new ClassCastException()}
    String asFieldName() { fieldName }
    Constant evaluate(final Scan s) { s.getVal(fieldName) }
    boolean appliesTo(final Schema s) { s.hasField(fieldName) }
    @Override String toString() { fieldName }
}
