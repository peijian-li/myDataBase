package index;

import lombok.AllArgsConstructor;
import lombok.Data;
import storage.PageId;

@Data
public class BTreePageId implements PageId {

    public final static int ROOT_PTR = 0;
    public final static int INTERNAL = 1;
    public final static int LEAF = 2;
    public final static int HEADER = 3;

    private final int tableId;
    private final int pageNumber;
    private final int pageCategory;

    public static String categoryToString(int category) {
        switch (category) {
            case ROOT_PTR:
                return "ROOT_PTR";
            case INTERNAL:
                return "INTERNAL";
            case LEAF:
                return "LEAF";
            case HEADER:
                return "HEADER";
            default:
                throw new IllegalArgumentException("category");
        }
    }

    public int[] serialize() {
        int[] data = new int[3];
        data[0] = tableId;
        data[1] = pageNumber;
        data[2] = pageCategory;
        return data;
    }



}
