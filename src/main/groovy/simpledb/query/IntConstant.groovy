package simpledb.query

import groovy.transform.CompileStatic
import groovy.transform.Immutable

@Immutable @CompileStatic
class IntConstant implements Constant {
    Integer val

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
