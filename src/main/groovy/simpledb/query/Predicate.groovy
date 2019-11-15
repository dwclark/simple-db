package simpledb.query

import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.transform.ToString
import simpledb.plan.Plan

@Immutable
@CompileStatic @ToString(includeNames=true, includePackage=false)
class Predicate implements java.util.function.Predicate<Scan> {

    final static Predicate EMPTY = new Predicate([])
    
    List<Term> terms

    Predicate conjoinWith(final Predicate p) {
        List<Term> copy = new ArrayList<>(terms)
        copy.addAll(p.terms)
        return new Predicate(copy)
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
