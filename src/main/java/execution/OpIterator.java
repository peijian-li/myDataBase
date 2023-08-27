package execution;

import common.DbException;
import common.TransactionAbortedException;
import storage.Tuple;
import storage.TupleDesc;

import java.io.Serializable;
import java.util.NoSuchElementException;

public interface OpIterator extends Serializable {

    void open() throws DbException, TransactionAbortedException;

    boolean hasNext() throws DbException, TransactionAbortedException;

    Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;


    void rewind() throws DbException, TransactionAbortedException;

    TupleDesc getTupleDesc();

    void close();
}
