package simpledb.query

import groovy.transform.CompileStatic
import simpledb.plan.Plan

class Predicate implements java.util.function.Predicate<Scan> {

    boolean test(final Scan s) {
        throw new UnsupportedOperationException()
    }

    int reductionFactor(Plan p) {
        throw new UnsupportedOperationException()
    }

    Constant equatesWithConstant(final String fieldName) {
        throw new UnsupportedOperationException()
    }
}
