package execution;

import common.DbException;
import common.TransactionAbortedException;
import common.Type;
import storage.Tuple;
import storage.TupleDesc;

import java.util.List;
import java.util.NoSuchElementException;

public class Project extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private final List<Integer> outFieldIds;


    public Project(List<Integer> fieldList, List<Type> typesList, OpIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childTupleDesc = child.getTupleDesc();
        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childTupleDesc.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(typesList.toArray(new Type[]{}), fieldAr);
    }


    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }


    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!child.hasNext()) return null;
        Tuple t = child.next();
        Tuple newTuple = new Tuple(td);
        newTuple.setRecordId(t.getRecordId());
        for (int i = 0; i < td.numFields(); i++) {
            newTuple.setField(i, t.getField(outFieldIds.get(i)));
        }
        return newTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}