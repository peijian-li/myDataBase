package index;

import common.Database;
import common.DbException;
import common.Type;
import storage.BufferPool;
import storage.Page;
import storage.PageId;
import storage.TupleDesc;
import transaction.TransactionId;

public abstract class BTreePage implements Page {
    public static final int INDEX_SIZE = Type.INT_TYPE.getLen();//索引大小

    private boolean dirty = false;//脏页标记
    private TransactionId dirtier = null;//修改为脏页的事务id

    protected final BTreePageId pid; //当前节点的BTreePageId
    protected final TupleDesc td;  //tuple描述
    protected final int keyField;  //索引字段下标
    protected int parent; //当前page的父page，如果是根节点那么就是0

    protected byte[] oldData;//存储旧的数据

    public BTreePage(BTreePageId id, int key) {
        this.pid = id;
        this.keyField = key;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
    }

    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len];
    }

    public BTreePageId getParentId() {
        if(parent == 0) {
            return BTreeRootPtrPage.getId(pid.getTableId());
        }
        return new BTreePageId(pid.getTableId(), parent, BTreePageId.INTERNAL);
    }

    public void setParentId(BTreePageId id) throws DbException {
        if(id == null) {
            throw new DbException("parent id must not be null");
        }
        if(id.getTableId() != pid.getTableId()) {
            throw new DbException("table id mismatch in setParentId");
        }
        if(id.getPageCategory() != BTreePageId.INTERNAL && id.getPageCategory() != BTreePageId.ROOT_PTR) {
            throw new DbException("parent must be an internal node or root pointer");
        }
        if(id.getPageCategory() == BTreePageId.ROOT_PTR) {
            parent = 0;
        }
        else {
            parent = id.getPageNumber();
        }
    }


    @Override
    public BTreePageId getId() {
        return pid;
    }

    @Override
    public TransactionId isDirty() {
        if (this.dirty)
            return this.dirtier;
        else
            return null;
    }

    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        if (dirty)
            this.dirtier = tid;
    }

    public abstract int getNumEmptySlots();


    public abstract boolean isSlotUsed(int i);


}
