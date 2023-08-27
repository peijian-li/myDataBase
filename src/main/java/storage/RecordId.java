package storage;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RecordId {

    private static final long serialVersionUID = 1L;

    private PageId pageId;
    private Integer tupleNo;

    @Override
    public boolean equals(Object o) {
        if(o instanceof RecordId){
            RecordId temp = (RecordId) o;
            if(temp.tupleNo.equals(this.tupleNo) && temp.pageId.equals(this.pageId)){
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (tupleNo+"").hashCode() + (pageId+"").hashCode();

    }

    @Override
    public String toString() {
        return "RecordId{" +
                "pageId=" + pageId +
                ", tupleno=" + tupleNo +
                '}';
    }
}
