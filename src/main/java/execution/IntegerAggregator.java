package execution;

import common.Type;
import storage.Field;
import storage.HeapPage;
import storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntegerAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;

    private int aggregateField;
    private int groupField;
    private Type groupFieldType;
    private Op aop;
    private Map<Field, List<Field>> group;//key:分组字段 value:聚合字段集合

    public IntegerAggregator(int groupField, Type groupFieldType, int aggregateField, Op aop) {
        this.aggregateField=aggregateField;
        this.groupField=groupField;
        this.groupFieldType=groupFieldType;
        this.aop=aop;
        group=new HashMap<>();
    }

    /**
     * 分组
     * @param tuple
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tuple) {
        Field aggregateField = tuple.getField(this.aggregateField);
        Field groupField = null;
        if(this.groupField!=-1){
            groupField = tuple.getField(this.groupField);
        }
        if(this.group.containsKey(groupField)){
            group.get(groupField).add(aggregateField);
        }else{
            List<Field> list = new ArrayList<>();
            list.add(aggregateField);
            group.put(groupField,list);
        }

    }

    @Override
    public OpIterator iterator() {
        return new AggregateIter(group,groupField,groupFieldType,aop);
    }
}
