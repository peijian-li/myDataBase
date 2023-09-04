package index;

import common.DbException;
import common.Type;
import lombok.extern.slf4j.Slf4j;
import storage.BufferPool;
import storage.Field;
import storage.IntField;
import storage.Page;
import transaction.TransactionId;

import java.io.*;
import java.util.Arrays;

@Slf4j
public class BTreeHeaderPage implements Page {
    public static final int INDEX_SIZE = Type.INT_TYPE.getLen();//索引大小

    private boolean dirty = false;//脏页标记
    private TransactionId dirtier = null;//修改为脏页的事务id

    private BTreePageId pid;  //当前节点的BTreePageId
    private byte[] header;  //记录每一个page的使用情况，对应一个个pageNumber
    private int numSlots;   //记录能存储的page使用情况数量

    private int nextPage; // 下一个header page的pageNumber，如果是最后一个，就是0
    private int prevPage; // 上一个header page的pageNumber，如果是第一个，就是0

    private byte[] oldData;//存储旧的数据

    public BTreeHeaderPage(BTreePageId id, byte[] data) throws IOException {
        this.pid = id;
        this.numSlots = getNumSlots();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.nextPage = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.prevPage = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        dis.close();

        setBeforeImage();
    }

    public void init() {
        Arrays.fill(header, (byte) 0xFF);
    }


    private static int getHeaderSize() {
        int pointerBytes = 2 * INDEX_SIZE;
        return BufferPool.getPageSize() - pointerBytes;
    }


    public static int getNumSlots() {
        return getHeaderSize() * 8;
    }


    public BTreeHeaderPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(this)
            {
                oldDataRef = oldData;
            }
            return new BTreeHeaderPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(this)
        {
            oldData = getPageData().clone();
        }
    }

    public BTreePageId getId() {
        return pid;
    }


    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // write out the next and prev pointers
        try {
            dos.writeInt(nextPage);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dos.writeInt(prevPage);

        } catch (IOException e) {
            e.printStackTrace();
        }

        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }


    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    public BTreePageId getPrevPageId() {
        if(prevPage == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), prevPage, BTreePageId.HEADER);
    }


    public BTreePageId getNextPageId() {
        if(nextPage == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), nextPage, BTreePageId.HEADER);
    }


    public void setPrevPageId(BTreePageId id) throws DbException {
        if(id == null) {
            prevPage = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setPrevPageId");
            }
            if(id.getPageCategory() != BTreePageId.HEADER) {
                throw new DbException("prevPage must be a header page");
            }
            prevPage = id.getPageNumber();
        }
    }


    public void setNextPageId(BTreePageId id) throws DbException {
        if(id == null) {
            nextPage = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setNextPageId");
            }
            if(id.getPageCategory() != BTreePageId.HEADER) {
                throw new DbException("nextPage must be a header page");
            }
            nextPage = id.getPageNumber();
        }
    }

    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        if (dirty) this.dirtier = tid;
    }


    public TransactionId isDirty() {
        if (this.dirty)
            return this.dirtier;
        else
            return null;
    }


    public boolean isSlotUsed(int i) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;
        return (header[headerbyte] & (1 << headerbit)) != 0;
    }

    public void markSlotUsed(int i, boolean value) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;

        log.info( "BTreeHeaderPage.setSlot: setting slot {} to {}", i, value);
        if(value)
            header[headerbyte] |= 1 << headerbit;
        else
            header[headerbyte] &= (0xFF ^ (1 << headerbit));
    }


    public int getEmptySlot() {
        for (int i=0; i<header.length; i++) {
            if((int) header[i] != 0xFF) {
                for(int j = 0; j < 8; j++) {
                    if(!isSlotUsed(i*8 + j)) {
                        return i*8 + j;
                    }
                }
            }
        }
        return -1;
    }
}
