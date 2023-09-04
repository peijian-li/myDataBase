package heap;

import lombok.AllArgsConstructor;
import lombok.Data;
import storage.PageId;

@Data
@AllArgsConstructor
public class HeapPageId implements PageId {

    private int tableId;
    private int pageNumber;

    public int[] serialize() {
        int[] data = new int[2];
        data[0] = getTableId();
        data[1] = getPageNumber();
        return data;
    }

}
