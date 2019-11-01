package simpledb.buffer

import simpledb.file.Page

interface PageFormatter {
    void format(Page p)
}
