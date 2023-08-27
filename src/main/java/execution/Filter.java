package execution;

import common.DbException;
import common.TransactionAbortedException;
import lombok.Getter;
import storage.Tuple;
import storage.TupleDesc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


public class Filter extends Operator{
    private static final long serialVersionUID = 1L;

    @Getter
    private Predicate predicate;
    private OpIterator child;//过滤前tuple集合迭代器
    @Getter
    private TupleDesc tupleDesc;
    private List<Tuple> childTuple;//保存过滤后的tuple
    private Iterator<Tuple> iterator;//过滤后tuple集合迭代器

    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.child = child;
        tupleDesc=child.getTupleDesc();
        childTuple=new ArrayList<>();
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child.open();
        while(child.hasNext()){
            Tuple next = child.next();
            if(predicate.filter(next)){
                childTuple.add(next);
            }
        }
        iterator = childTuple.iterator();
        super.open();
    }

    public void close() {
        child.close();
        iterator = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator = childTuple.iterator();
    }


    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        if(iterator!=null && iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
