package index;

import common.DbException;
import storage.Page;
import transaction.TransactionId;

import java.io.*;

public class BTreeRootPtrPage implements Page {
    private final static int PAGE_SIZE = 9;

    private boolean dirty = false;//脏页标记
    private TransactionId dirtier = null;//修改为脏页的事务id

    private final BTreePageId pid;//当前节点的BTreePageId

    private int root; //保存当前根节点的pageNumber
    private int rootCategory;  //保存当前根节点的类型，INTERNAL或LEAF，当只有一个节点时，就是LEAF
    private int header;  //保存当前header页的pageNumber



    public BTreeRootPtrPage(BTreePageId id, byte[] data) throws IOException {
        this.pid = id;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        root = dis.readInt();
        rootCategory = dis.readByte();
        header = dis.readInt();

    }



    public BTreePageId getId() {
        return pid;
    }

    public byte[] getPageData(){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(PAGE_SIZE);
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        try{
            dos.writeInt(root);
        }catch(IOException e){
            e.printStackTrace();
        }

        try{
            dos.writeByte((byte) rootCategory);
        }catch(IOException e){
            e.printStackTrace();
        }


        try{
            dos.writeInt(header);
        }catch(IOException e){
            e.printStackTrace();
        }

        try {
            dos.flush();
        }catch(IOException e) {
            e.printStackTrace();
        }

        return byteArrayOutputStream.toByteArray();
    }


    public void markDirty(boolean dirty, TransactionId tid){
        this.dirty = dirty;
        if (dirty) this.dirtier = tid;
    }

    public TransactionId isDirty() {
        if (this.dirty)
            return this.dirtier;
        else
            return null;
    }



    public static BTreePageId getId(int tableId) {
        return new BTreePageId(tableId, 0, BTreePageId.ROOT_PTR);
    }

    public static byte[] createEmptyPageData() {
        return new byte[PAGE_SIZE];
    }

    public BTreePageId getRootId() {
        if(root == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), root, rootCategory);
    }


    public void setRootId(BTreePageId id) throws DbException {
        if(id == null) {
            root = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setRootId");
            }
            if(id.getPageCategory() != BTreePageId.INTERNAL && id.getPageCategory() != BTreePageId.LEAF) {
                throw new DbException("root must be an internal node or leaf node");
            }
            root = id.getPageNumber();
            rootCategory = id.getPageCategory();
        }
    }


    public BTreePageId getHeaderId() {
        if(header == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), header, BTreePageId.HEADER);
    }


    public void setHeaderId(BTreePageId id) throws DbException {
        if(id == null) {
            header = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setHeaderId");
            }
            if(id.getTableId() != BTreePageId.HEADER) {
                throw new DbException("header must be of type BTreePageId.HEADER");
            }
            header = id.getPageNumber();
        }
    }


    public static int getPageSize() {
        return PAGE_SIZE;
    }
}
