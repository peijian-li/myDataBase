package index;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import storage.Field;
import storage.RecordId;

import java.io.Serializable;

@Getter
@Setter
public class BTreeEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private Field key;  //内部节点中的key

    private BTreePageId leftChild;  //左孩子的BTreePageId

    private BTreePageId rightChild;  //右孩子的BTreePageId

    private RecordId recordId;

    public BTreeEntry(Field key, BTreePageId leftChild, BTreePageId rightChild) {
        this.key = key;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }


}
