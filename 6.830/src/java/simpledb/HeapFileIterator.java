package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private HeapFile heapFile;
    private TransactionId transactionId;
    private final int tableId;
    private int pageId;
    private int numPages;

    private Iterator<Tuple> tupleIterator;

    HeapFileIterator(HeapFile file, TransactionId tid) {
        heapFile = file;
        transactionId = tid;
        tableId = this.heapFile.getId();
        numPages = this.heapFile.numPages();
        pageId = 0;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pageId = 0;
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(transactionId, new HeapPageId(tableId, pageId), null);
        tupleIterator = heapPage.iterator();
    }



    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            return false;
        }
        if (tupleIterator.hasNext()) {
            return true;
        }
        while (pageId + 1 < numPages) {
            ++pageId;
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(transactionId, new HeapPageId(tableId, pageId), null);
            tupleIterator = heapPage.iterator();
            if (tupleIterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (tupleIterator == null) {
            throw new NoSuchElementException();
        }
        if (tupleIterator.hasNext()) {
            return tupleIterator.next();
        }
        while (pageId + 1 < numPages) {
            ++pageId;
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(transactionId, new HeapPageId(tableId, pageId), null);
            tupleIterator = heapPage.iterator();
            if (tupleIterator.hasNext()) {
                return tupleIterator.next();
            }
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        tupleIterator = null;
    }
}
