package optimizer;

import common.DbException;
import common.TransactionAbortedException;
import execution.OpIterator;
import execution.Operator;
import storage.Tuple;
import storage.TupleDesc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class OrderBy extends Operator {
    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private final List<Tuple> childTups = new ArrayList<>();
    private final int orderByField;
    private final String orderByFieldName;
    private Iterator<Tuple> it;
    private final boolean asc;


    public OrderBy(int orderbyField, boolean asc, OpIterator child) {
        this.child = child;
        td = child.getTupleDesc();
        this.orderByField = orderbyField;
        this.orderByFieldName = td.getFieldName(orderbyField);
        this.asc = asc;
    }

    public boolean isASC()
    {
        return this.asc;
    }

    public int getOrderByField()
    {
        return this.orderByField;
    }

    public String getOrderFieldName()
    {
        return this.orderByFieldName;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child.open();
        while (child.hasNext())
            childTups.add(child.next());
        childTups.sort(new TupleComparator(orderByField, asc));
        it = childTups.iterator();
        super.open();
    }

    public void close() {
        super.close();
        it = null;
    }

    public void rewind() {
        it = childTups.iterator();
    }

    protected Tuple fetchNext() throws NoSuchElementException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
