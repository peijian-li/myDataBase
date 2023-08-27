package storage;

import common.DbException;
import common.TransactionAbortedException;

import java.util.NoSuchElementException;

public interface DbFileIterator {

    void open() throws DbException, TransactionAbortedException;

    boolean hasNext() throws DbException, TransactionAbortedException;

    Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

    void rewind() throws DbException, TransactionAbortedException;

    void close();
}
