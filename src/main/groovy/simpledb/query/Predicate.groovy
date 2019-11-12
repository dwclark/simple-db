package simpledb.query

import groovy.transform.CompileStatic
import groovy.transform.ToString
import simpledb.plan.Plan

@CompileStatic @ToString(includeNames=true, includePackage=false)
class Predicate implements java.util.function.Predicate<Scan> {

    private final List<Term> terms

    Predicate() {
        terms = []
    }

    Predicate(Term term) {
        this()
        terms.add(term)
    }

    void conjoinWith(final Predicate p) {
        terms.addAll(p.terms)
    }
    
    boolean test(final Scan s) {
        return terms.every { Term t -> t.test(s) }
    }

    int reductionFactor(Plan p) {
        int ret = 1
        for(Term t : terms) {
            ret *= t.reductionFactor(p)
        }

        return ret
    }

    Constant equatesWithConstant(final String fieldName) {
        terms.findResult { Term t -> t.equatesWithConstant(fieldName) }
    }

    String equatesWithField(final String fieldName) {
        terms.findResult { Term t -> t.equatesWithField(fieldName) }
    }
    
}
