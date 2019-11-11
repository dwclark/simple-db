package simpledb.query

import groovy.transform.CompileStatic

@CompileStatic
class StringConstant implements Constant {
    final String val

    StringConstant(final String val) {
        this.val = val
    }

    @Override
    boolean equals(final Object o) {
        if(!(o instanceof StringConstant)) {
            return false
        }
        else {
            StringConstant rhs = (StringConstant) o
            return Objects.equals(val, rhs.val)
        }
    }

    @Override
    int compareTo(final Constant c) {
        if(!(c instanceof StringConstant)) {
            throw new ClassCastException()
        }
        
        return val.compareTo(((StringConstant) c).val)
    }

    @Override
    int hashCode() {
        return val.hashCode()
    }

    @Override
    String toString() {
        return val
    }
}
