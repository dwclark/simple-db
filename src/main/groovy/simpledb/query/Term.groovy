package simpledb.query

import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import simpledb.plan.Plan
import simpledb.record.Schema

@CompileStatic @TupleConstructor @ToString(includeNames=true, includePackage=false)
class Term implements java.util.function.Predicate<Scan> {
    final Expression lhs
    final Expression rhs

    int reductionFactor(final Plan p) {
        String rhsName, lhsName
        if(lhs.fieldName && rhs.fieldName) {
            return Math.max(p.distinctValues(lhs.asFieldName()),
                            p.distinctValues(rhs.asFieldName()))
        }

        if(lhs.fieldName) {
            return p.distinctValues(lhs.asFieldName())
        }

        if(rhs.fieldName) {
            return p.distinctValues(rhs.asFieldName())
        }

        if(lhs.asConstant() == rhs.asConstant()) {
            return 1
        }
        else {
            return Integer.MAX_VALUE
        }
    }

    Constant equatesWithConstant(final String fieldName) {
        if(lhs.fieldName && rhs.constant && lhs.asFieldName() == fieldName) {
            return rhs.asConstant()
        }
        else if(rhs.fieldName && lhs.constant && rhs.asFieldName() == fieldName) {
            return lhs.asConstant()
        }
        else {
            return null
        }
    }

    String equatesWithField(final String fieldName) {
        if(lhs.fieldName && rhs.fieldName && lhs.asFieldName() == fieldName) {
            return rhs.asFieldName()
        }
        else if(rhs.fieldName && lhs.fieldName && rhs.asFieldName() == fieldName) {
            return lhs.asFieldName()
        }
        else {
            return null
        }
    }

    boolean appliesTo(final Schema s) {
        return lhs.appliesTo(s) && rhs.appliesTo(s)
    }

    boolean test(final Scan s) {
        return lhs.evaluate(s) == rhs.evaluate(s)
    }
}
