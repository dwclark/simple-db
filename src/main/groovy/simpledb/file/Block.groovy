package simpledb.file

import groovy.transform.*
import java.util.concurrent.atomic.AtomicInteger

@CompileStatic
@EqualsAndHashCode
@ToString(includeNames=true, includePackage=false)
class Block {
    private static final String UNASSIGNED = 'unassigned_Hs7SDm/tPuMTjhwgBFG/VboyNb0='
    private static final AtomicInteger unassignedCounter = new AtomicInteger()
    
    final String fileName
    final int number

    Block(final String fileName, final int number) {
        this.fileName = fileName
        this.number = number
    }

    static Block unassigned() {
        return new Block(UNASSIGNED, unassignedCounter.incrementAndGet())
    }
}
