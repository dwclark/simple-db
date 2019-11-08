package simpledb.record

import groovy.transform.CompileStatic
import simpledb.tx.Transaction
import simpledb.file.Block

@CompileStatic
class RecordFile {
    final TableInfo tableInfo
    final Transaction transaction
    final String fileName

    private RecordPage recordPage
    private int blockNumber

    RecordFile(final TableInfo tableInfo, final Transaction transaction) {
        this.tableInfo = tableInfo
        this.transaction = transaction
        fileName = tableInfo.fileName
        if(transaction.size(fileName) == 0) {
            appendBlock()
        }

        moveTo(0)
    }

    void beforeFirst() {
        moveTo(0)
    }

    void close() {
        recordPage.close()
    }

    boolean next() {
        while(true) {
            if(recordPage.next()) {
                return true;
            }

            if(lastBlock) {
                return false
            }

            moveTo(blockNumber + 1)
        }
    }

    int getInt(final String fieldName) {
        return recordPage.getInt(fieldName)
    }

    void setInt(final String fieldName, final int val) {
        recordPage.setInt(fieldName, val)
    }

    String getString(final String fieldName) {
        recordPage.getString(fieldName)
    }

    void setString(final String fieldName, final String val) {
        recordPage.setString(fieldName, val)
    }

    void delete() {
        recordPage.delete()
    }

    void insert() {
        while(!recordPage.insert()) {
            if(lastBlock) {
                appendBlock()
            }

            moveTo(blockNumber + 1)
        }
    }

    void moveToRid(final RID rid) {
        moveTo(rid.blockNumber)
        recordPage.currentSlot = rid.id
    }

    RID getCurrentRid() {
        return new RID(blockNumber, recordPage.currentSlot)
    }

    private void moveTo(final int b) {
        if(recordPage != null)  {
            recordPage.close()
        }

        blockNumber = b
        Block block = new Block(fileName, blockNumber)
        recordPage = new RecordPage(block, tableInfo, transaction)
    }

    private boolean getLastBlock() {
        return blockNumber == transaction.size(fileName) - 1
    }

    private void appendBlock(){
        transaction.append(fileName, new RecordFormatter(tableInfo, transaction.bufferManager.fileManager.pageSize))
    }
}
