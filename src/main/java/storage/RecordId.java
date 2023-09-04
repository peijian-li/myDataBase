package storage;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RecordId {

    private static final long serialVersionUID = 1L;

    private PageId pageId;
    private Integer tupleNumber;

    @Override
    public boolean equals(Object o) {
        if(o instanceof RecordId){
            RecordId temp = (RecordId) o;
            if(temp.tupleNumber.equals(this.tupleNumber) && temp.pageId.equals(this.pageId)){
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (tupleNumber+"").hashCode() + (pageId+"").hashCode();

    }

    @Override
    public String toString() {
        return "RecordId{" +
                "pageId=" + pageId +
                ", tupleno=" + tupleNumber +
                '}';
    }
}
