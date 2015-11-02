package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId transactionId;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private Boolean hasDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.transactionId = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.hasDeleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasDeleted) {
            return null;
        }
        hasDeleted = true;
        int numDeletes = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, tuple);
                ++numDeletes;
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        Tuple ret = new Tuple(tupleDesc);
        ret.setField(0, new IntField(numDeletes));
        return ret;
    }
}
