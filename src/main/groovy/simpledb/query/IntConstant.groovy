package simpledb.query

import groovy.transform.CompileStatic

@CompileStatic
class IntConstant implements Constant {
    final Integer val

    IntConstant(final Integer val) {
        this.val = val
    }

    @Override
    boolean equals(final Object o) {
        if(!(o instanceof IntConstant)) {
            return false
        }
        else {
            IntConstant rhs = (IntConstant) o
            return Objects.equals(val, rhs.val)
        }
    }

    @Override
    int compareTo(final Constant c) {
        if(!(c instanceof IntConstant)) {
            throw new ClassCastException()
        }
        
        return val.compareTo(((IntConstant) c).val)
    }

    @Override
    int hashCode() {
        return val.hashCode()
    }

    @Override
    String toString() {
        return val.toString()
    }
}
