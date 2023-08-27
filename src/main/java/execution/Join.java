package execution;

import common.DbException;
import common.TransactionAbortedException;
import lombok.Getter;
import storage.Field;
import storage.Tuple;
import storage.TupleDesc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Join extends Operator{
    private static final long serialVersionUID = 1L;

    @Getter
    private JoinPredicate joinPredicate;
    private OpIterator child1;
    private OpIterator child2;
    @Getter
    private TupleDesc tupleDesc;
    private final List<Tuple> childTuple = new ArrayList<>();
    private Iterator<Tuple> it;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.tupleDesc = TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }


    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child1.open();
        child2.open();
        //合并child1和child2中tuple
        while(child1.hasNext()){
            Tuple next1 = child1.next();
            while(child2.hasNext()){
                Tuple next2 = child2.next();
                if(joinPredicate.filter(next1,next2)){
                    Tuple mergeTuple = new Tuple(tupleDesc);
                    Iterator<Field> fields1 = next1.fields();
                    Iterator<Field> fields2 = next2.fields();
                    int count = 0;
                    while(fields1.hasNext()){
                        mergeTuple.setField(count++,fields1.next());
                    }
                    while(fields2.hasNext()){
                        mergeTuple.setField(count++,fields2.next());
                    }
                    childTuple.add(mergeTuple);
                }
            }
            child2.rewind();
        }
        it = childTuple.iterator();
        super.open();
    }

    public void close() {
        child1.close();
        child2.close();
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = childTuple.iterator();
    }


    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(it!=null && it.hasNext()){
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }
}
