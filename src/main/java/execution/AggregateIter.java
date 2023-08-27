package execution;

import common.DbException;
import common.TransactionAbortedException;
import common.Type;
import storage.Field;
import storage.IntField;
import storage.Tuple;
import storage.TupleDesc;

import java.util.*;

public class AggregateIter implements OpIterator {
    private static final long serialVersionUID = 1L;

    private List<Tuple> resultSet;  //聚合结果tuple集合
    private Iterator<Tuple> tupleIterator;//聚合结果tuple集合迭代器
    private TupleDesc tupleDesc;//聚合结果tuple描述
    private Map<Field, List<Field>> group;
    private Aggregator.Op aop;
    private int groupField;
    private Type groupFieldType;


    public AggregateIter(Map<Field, List<Field>> group, int groupField, Type groupFieldType, Aggregator.Op aop) {
        this.group = group;
        this.groupField = groupField;
        this.groupFieldType=groupFieldType;
        this.aop = aop;

        //生成聚合结果tuple描述
        //有分组字段时，第一个字段为分组字段，第二个为int字段，记录分组聚合结果
        //无分组字段时，只有一个int字段，记录全组聚合结果
        Type[] type;
        if(groupField!=-1){
            type = new Type[2];
            type[0] = groupFieldType;
            type[1] = Type.INT_TYPE;
        }else{
            type = new Type[1];
            type[0] = Type.INT_TYPE;
        }
        this.tupleDesc = new TupleDesc(type);
    }

    /**
     * 计算聚合结果，生成迭代器
     * @throws DbException
     * @throws TransactionAbortedException
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.resultSet  = new ArrayList<>();
        if(aop == Aggregator.Op.COUNT){
            for(Field field:group.keySet()){
                Tuple tuple = new Tuple(this.tupleDesc);
                if(field!=null){
                    tuple.setField(0,field);
                }
                tuple.setField(1,new IntField(group.get(field).size()));
                this.resultSet.add(tuple);
            }
        }else if(aop == Aggregator.Op.MIN){
            for(Field field:group.keySet()){
                int min = Integer.MAX_VALUE;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.group.get(field).size();i++){
                    IntField field1 = (IntField)group.get(field).get(i);
                    if(field1.getValue()<min){
                        min = field1.getValue();
                    }
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(min));
                }else{
                    tuple.setField(0,new IntField(min));
                }
                resultSet.add(tuple);
            }
        }else if(aop == Aggregator.Op.MAX){
            for(Field field:group.keySet()){
                int max = Integer.MIN_VALUE;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.group.get(field).size();i++){
                    IntField field1 = (IntField)group.get(field).get(i);
                    if(field1.getValue()>max){
                        max = field1.getValue();
                    }
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(max));
                }else{
                    tuple.setField(0,new IntField(max));
                }
                resultSet.add(tuple);
            }
        }else if(aop == Aggregator.Op.AVG){
            for(Field field: this.group.keySet()){
                int sum = 0;
                int size = this.group.get(field).size();
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<size;i++){
                    IntField field1 = (IntField) group.get(field).get(i);
                    sum += field1.getValue();
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(sum/size));
                }else{
                    tuple.setField(0,new IntField(sum/size));
                }
                resultSet.add(tuple);
            }
        }else if(aop == Aggregator.Op.SUM){
            for(Field field:this.group.keySet()){
                int sum = 0;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.group.get(field).size();i++){
                    IntField field1  = (IntField) this.group.get(field).get(i);
                    sum += field1.getValue();
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(sum));
                }else{
                    tuple.setField(0,new IntField(sum));
                }
                resultSet.add(tuple);
            }
        }
        this.tupleIterator = resultSet.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(tupleIterator==null){
            return false;
        }
        return tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if(resultSet!=null){
            tupleIterator = resultSet.iterator();
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.tupleIterator = null;
    }
}
