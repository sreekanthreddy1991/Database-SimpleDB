package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate filterPredicate;
    private DbIterator filterChild;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        filterPredicate = p;
        filterChild = child;
    }

    public Predicate getPredicate() {
        return filterPredicate;
    }

    public TupleDesc getTupleDesc() {
        return filterChild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        filterChild.open();
        super.open();
    }

    public void close() {
        super.close();
        filterChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        filterChild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(filterChild.hasNext()){
            Tuple t = filterChild.next();
            if (filterPredicate.filter(t))
                return t;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { filterChild };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(children.length > 0){
            filterChild = children[0];
        }
    }

}
