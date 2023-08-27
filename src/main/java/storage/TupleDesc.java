package storage;

import common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TupleDesc implements Serializable{

    private static final long serialVersionUID = 1L;

    private List<TDItem> tupleDescList = new ArrayList<>();


    private static class TDItem implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Type fieldType;
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        private String getFieldName(){
            return fieldName;
        }

        private Type getFieldType(){
            return fieldType;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }


    }

    public Iterator<TDItem> iterator() {
        return this.tupleDescList.iterator();
    }

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if(typeAr.length==fieldAr.length){
            for(int i=0;i<typeAr.length;i++){
                this.tupleDescList.add(new TDItem(typeAr[i],fieldAr[i]));
            }
        }
    }

    public TupleDesc(Type[] typeAr) {
        for(int i=0;i<typeAr.length;i++){
            this.tupleDescList.add(new TDItem(typeAr[i],null));
        }
    }

    public TupleDesc(List<TDItem> tdItems){
        this.tupleDescList = tdItems;
    }


    public int getItemLength(){
        return this.tupleDescList.size();
    }


    public int numFields() {
        return this.tupleDescList.size();
    }


    public String getFieldName(int i) throws NoSuchElementException {

        if(i<0 || i>=this.tupleDescList.size()){
            throw new NoSuchElementException();
        }
        return this.tupleDescList.get(i).fieldName;
    }

    public Type getFieldType(int i) throws NoSuchElementException {
        if(i<0 || i>=this.tupleDescList.size()){
            throw new NoSuchElementException();
        }
        return this.tupleDescList.get(i).fieldType;
    }


    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name==null){
            throw new NoSuchElementException();
        }
        for(int i=0;i<this.tupleDescList.size();i++){
            String fieldName = this.tupleDescList.get(i).getFieldName();
            if(fieldName==null){
                continue;
            }
            if(fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }


    public int getSize() {

        int res = 0;
        for(int i=0;i<this.tupleDescList.size();i++){
            res += this.tupleDescList.get(i).fieldType.getLen();
        }
        return res;

    }


    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> tupleDescList01 = td1.tupleDescList;
        List<TDItem> tupleDescList02 = td2.tupleDescList;
        List<TDItem> res = new ArrayList<>(tupleDescList01);
        res.addAll(tupleDescList02);
        return new TupleDesc(res);
    }


    public boolean equals(Object o) {
        if (o instanceof TupleDesc){
            List<TDItem> tupleDescList = ((TupleDesc) o).tupleDescList;
            if(tupleDescList.size()==this.tupleDescList.size()){
                for(int i=0;i<this.tupleDescList.size();i++){
                    if(!this.tupleDescList.get(i).getFieldType().equals(tupleDescList.get(i).getFieldType())){
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    public int hashCode() {
        throw new UnsupportedOperationException("unimplemented");
    }

    public String toString() {
        return "TupleDesc{" +
                "tupleDescList=" + tupleDescList +
                '}';
    }



}
