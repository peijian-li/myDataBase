package execution;

import common.DbException;
import common.TransactionAbortedException;
import lombok.Getter;
import lombok.Setter;
import storage.Tuple;
import storage.TupleDesc;

import java.util.NoSuchElementException;

public abstract class Operator implements OpIterator {
    private static final long serialVersionUID = 1L;

    private Tuple next = null;
    private boolean open = false;
    @Getter
    @Setter
    private int estimatedCardinality = 0;

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!this.open) {
            throw new IllegalStateException("Operator not yet open");
        }
        if (next == null) {
            next = fetchNext();
        }
        return next != null;
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (next == null) {
            next = fetchNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
        }
        Tuple result = next;
        next = null;
        return result;
    }

    protected abstract Tuple fetchNext() throws DbException, TransactionAbortedException;

    public void close() {
        next = null;
        this.open = false;
    }


    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
    }

    public abstract OpIterator[] getChildren();

    public abstract void setChildren(OpIterator[] children);

    public abstract TupleDesc getTupleDesc();
}
