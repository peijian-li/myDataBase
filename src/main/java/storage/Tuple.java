package storage;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Getter
@Setter
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;//tuple描述
    private List<Field> fieldList;//字段集合
    private RecordId recordId;//tuple物理位置

    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        this.fieldList = new ArrayList<>(td.numFields());
    }

    public Iterator<Field> fields() {
        return this.fieldList.iterator();
    }

    public void setField(int i, Field f) {
        if(i>=this.fieldList.size()){
            this.fieldList.add(i,f);
        }else{
            this.fieldList.set(i,f);
        }
    }

    public Field getField(int i) {
        if(i<0 || i>= this.fieldList.size()){
            return null;
        }
        return this.fieldList.get(i);
    }

    public String toString() {
        return "Tuple{" +
                "tupleDesc=" + tupleDesc +
                ", fieldList=" + fieldList +
                '}';

    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            return ((Tuple) o).tupleDesc.equals(tupleDesc) && ((Tuple) o).recordId.equals(recordId) && ((Tuple) o).fieldList.equals(fieldList);
        }
        return false;
    }


}
