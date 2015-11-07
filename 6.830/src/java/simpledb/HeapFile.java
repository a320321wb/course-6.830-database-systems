package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private final AtomicInteger numOfPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        numOfPages = new AtomicInteger((int)file.length() / Database.getBufferPool().getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int PAGE_SIZE = Database.getBufferPool().getPageSize();
        HeapPageId pageId = new HeapPageId(pid.getTableId(), pid.pageNumber());
        HeapPage page = null;
        try {
            RandomAccessFile reader = new RandomAccessFile(file, "r");
            long offset = 1L * PAGE_SIZE * pid.pageNumber();
            reader.seek(offset);
            byte[] bytes = new byte[PAGE_SIZE];
            reader.read(bytes);
            page = new HeapPage(pageId, bytes);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        int PAGE_SIZE = Database.getBufferPool().getPageSize();
        try {
            RandomAccessFile writer = new RandomAccessFile(file, "rw");
            long offset = 1L * PAGE_SIZE * page.getId().pageNumber();
            writer.seek(offset);
            byte[] bytes = page.getPageData();
            writer.write(bytes);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numOfPages.get();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> ret = new ArrayList<Page>();
        for (int i = 0; i < numPages(); ++i) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                heapPage.insertTuple(t);
                ret.add(heapPage);
                return ret;
            }
            Database.getBufferPool().releasePage(tid, pageId);
        }

        HeapPageId newPageId = new HeapPageId(getId(), numOfPages.getAndIncrement());
        HeapPage heapPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        ret.add(heapPage);
        return ret;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        return heapPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

