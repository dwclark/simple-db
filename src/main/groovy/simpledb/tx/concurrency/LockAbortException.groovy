package simpledb.tx.concurrency

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

@CompileStatic
@InheritConstructors
class LockAbortException extends RuntimeException { }
