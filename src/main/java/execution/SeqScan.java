package execution;

import common.Database;
import common.DbException;
import common.TransactionAbortedException;
import common.Type;
import lombok.Getter;
import storage.DbFile;
import storage.DbFileIterator;
import storage.Tuple;
import storage.TupleDesc;
import transaction.TransactionId;

import java.util.NoSuchElementException;

@Getter
public class SeqScan implements OpIterator{
    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;
    private String tableAlias;
    private DbFileIterator dbFileIterator;

    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        this.transactionId = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(this.tableId);
        DbFileIterator iterator = dbFile.iterator(this.transactionId);
        this.dbFileIterator =  iterator;
        iterator.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.dbFileIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return this.dbFileIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.dbFileIterator.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc();
        int itemLen = tupleDesc.getItemLength();
        Type[] types = new Type[itemLen];
        String[] fieldNames = new String[itemLen];
        for(int i=0;i<itemLen;i++){
            types[i] = tupleDesc.getFieldType(i);
            fieldNames[i] = this.tableAlias +"."+ tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types,fieldNames);
    }

    @Override
    public void close() {
        this.dbFileIterator.close();
    }
}
