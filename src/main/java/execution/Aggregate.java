package execution;

import common.DbException;
import common.TransactionAbortedException;
import common.Type;
import storage.Tuple;
import storage.TupleDesc;

import java.util.NoSuchElementException;

public class Aggregate extends Operator{

    private static final long serialVersionUID = 1L;

    private OpIterator child;//分组前tuple集合迭代器
    private int aggregateField;//聚合字段
    private int groupField;//分组字段
    private Aggregator.Op aop;//聚合操作
    private Aggregator aggregator;//聚合器
    private OpIterator opIterator;//分组聚合后tuple集合迭代器

    public Aggregate(OpIterator child, int aggregateField, int groupField, Aggregator.Op aop) {
        this.child = child;
        this.aggregateField = aggregateField;
        this.groupField = groupField;
        this.aop = aop;
        Type fieldType = child.getTupleDesc().getFieldType(aggregateField);
        if(groupField!=-1){
            if(fieldType.equals(Type.INT_TYPE)){
                aggregator = new IntegerAggregator(groupField,child.getTupleDesc().getFieldType(groupField),aggregateField,aop);
            }else if(fieldType.equals(Type.STRING_TYPE)){
                aggregator = new StringAggregator(groupField,child.getTupleDesc().getFieldType(groupField),aggregateField,aop);
            }else{
                aggregator = null;
            }
        }else{
            if(fieldType.equals(Type.INT_TYPE)){
                aggregator = new IntegerAggregator(groupField,null,aggregateField,aop);
            }else if(fieldType.equals(Type.STRING_TYPE)){
                aggregator = new StringAggregator(groupField,null,aggregateField,aop);
            }else{
                aggregator = null;
            }
        }
        this.opIterator = aggregator.iterator();
    }

    public int groupField() {
        if(groupField==-1){
            return Aggregator.NO_GROUPING;
        }
        return groupField;
    }

    public String groupFieldName() {
        if(groupField==-1){
            return null;
        }
        try{
            return child.getTupleDesc().getFieldName(groupField);
        }catch (NoSuchElementException e){
            return null;
        }
    }

    public int aggregateField() {
        return aggregateField;
    }

    public String aggregateFieldName() {
        try{
            return child.getTupleDesc().getFieldName(aggregateField);
        }catch (NoSuchElementException e) {
            return null;
        }
    }

    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public String aggregateOpName() {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        while(child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        opIterator.open();
        super.open();
    }

    public void close() {
        child.close();
        opIterator.close();
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
        child.rewind();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(opIterator!=null && opIterator.hasNext()){
            return opIterator.next();
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

    @Override
    public TupleDesc getTupleDesc() {
        return opIterator.getTupleDesc();
    }
}
