package execution;

import common.Database;
import common.DbException;
import common.TransactionAbortedException;
import common.Type;
import storage.IntField;
import storage.Tuple;
import storage.TupleDesc;
import transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Delete extends Operator{
    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private ArrayList<Tuple> tupleList;
    private Iterator<Tuple> iterator;

    public Delete(TransactionId transactionId, OpIterator child) {
        this.transactionId = transactionId;
        this.child = child;
        tupleList = new ArrayList<>();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        int count = 0;
        while(child.hasNext()){
            Tuple next = child.next();
            count++;
            try {
                Database.getBufferPool().deleteTuple(transactionId,next);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0,new IntField(count));
        tupleList.add(tuple);
        iterator = tupleList.iterator();
        super.open();
    }

    public void close() {
        child.close();
        iterator = null;
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        iterator = tupleList.iterator();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(iterator != null && iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child=children[0];
    }

    @Override
    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }
}
