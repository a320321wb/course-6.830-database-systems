package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private TupleDesc tupleDesc;
    private HashMap<Field, Integer> numTuples;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        if (gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            this.tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }
        this.numTuples = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = gbField == NO_GROUPING?null:tup.getField(gbField);
        if (numTuples.containsKey(field)) {
            numTuples.put(field, numTuples.get(field) + 1);
        } else {
            numTuples.put(field, 1);
        }
    }

    public static class StringAggregatorIterator extends Operator {

        private static final long serialVersionUID = 1L;

        private StringAggregator stringAggregator;
        private Iterator<Field> iterator;

        StringAggregatorIterator(StringAggregator stringAggregator) {
            this.stringAggregator = stringAggregator;
            this.iterator = stringAggregator.numTuples.keySet().iterator();
        }

        public TupleDesc getTupleDesc() {
            return stringAggregator.tupleDesc;
        }

        public void open()
                throws DbException, NoSuchElementException, TransactionAbortedException {
            iterator = stringAggregator.numTuples.keySet().iterator();
        }

        public void close() {
            iterator = null;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            iterator = stringAggregator.numTuples.keySet().iterator();
        }

        protected Tuple fetchNext() throws TransactionAbortedException, DbException {
            if (iterator == null) {
                return null;
            }
            while (iterator.hasNext()) {
                Field field = iterator.next();
                int result = stringAggregator.numTuples.get(field);
                Tuple tuple = new Tuple(getTupleDesc());
                if (stringAggregator.gbField == NO_GROUPING) {
                    tuple.setField(0, new IntField(result));
                } else {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(result));
                }
                return tuple;
            }
            return null;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new StringAggregatorIterator(this);
    }

}
