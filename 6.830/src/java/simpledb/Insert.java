package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {

    private TransactionId transactionId;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean hasInserted;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableid;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.hasInserted = false;
        if (!Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc())) {
            throw new DbException("tupleDesc of child differs from table into which we are to insert");
        }
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (hasInserted) {
            return null;
        }
        hasInserted = true;
        int numInserts = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
                ++numInserts;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(numInserts));
        return ret;
    }
}
