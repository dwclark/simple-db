package simpledb.query;

import simpledb.record.Schema;

public interface Expression {
    boolean isConstant();
    boolean isFieldName();
    Constant asConstant();
    String asFieldName();
    Constant evaluate(Scan s);
    boolean appliesTo(Schema s);
}
