package simpledb.file

import groovy.transform.*

@CompileStatic
@EqualsAndHashCode
@ToString(includeNames=true, includePackage=false)
class Block {
    final String fileName
    final int number

    Block(final String fileName, final int number) {
        this.fileName = fileName
        this.number = number
    }
}
