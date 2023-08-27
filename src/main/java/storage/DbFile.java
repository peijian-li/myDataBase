package storage;

import common.DbException;
import common.TransactionAbortedException;
import transaction.TransactionId;

import java.io.IOException;
import java.util.List;

public interface DbFile {

    Page readPage(PageId pageId);

    void writePage(Page page) throws IOException;

    List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException;

    List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException;

    DbFileIterator iterator(TransactionId tid);

    int getId();

    TupleDesc getTupleDesc();

}
