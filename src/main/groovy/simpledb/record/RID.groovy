package simpledb.record

import groovy.transform.*

@CompileStatic
@EqualsAndHashCode
@ToString(includePackage=false, includeNames=true)
class RID {
    final int blockNumber
    final int id

    public RID(final int blockNumber, final int id) {
        this.blockNumber = blockNumber
        this.id = id
    }
}
