package heap;

import common.Database;
import common.DbException;
import storage.*;
import transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapPage implements Page {

    private final HeapPageId pid;
    private final TupleDesc td;
    private final byte[] header; //存储每个tuple状态，一个字节存储8个tuple状态
    private final Tuple[] tuples;
    private final int numSlots;//tuple数量

    private Boolean dirty;
    private TransactionId transactionId;

    public HeapPage(HeapPageId pid, byte[] data) throws IOException {
        this.pid = pid;
        this.td = Database.getCatalog().getTupleDesc(pid.getTableId());
        this.numSlots = getNumTuples();
        this.dirty = false;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++) {
            header[i] = dis.readByte();
        }
        tuples = new Tuple[numSlots];
        try{
            for (int i=0; i<tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();
    }

    /**
     * 插入tuple
     * @param t
     * @throws DbException
     */
    public void insertTuple(Tuple t) throws DbException {
        if(getNumEmptySlots()==0){
            throw new DbException("slots if empty");
        }
        //判断元数据是否正常
        if(!t.getTupleDesc().equals(this.td)){
            throw new DbException("insert tuple err");
        }
        for(int i=0;i<numSlots;i++){
            if(tuples[i]!=null){
                continue;
            }
            if(!isSlotUsed(i)){
                tuples[i]  =t;
                tuples[i].setRecordId(new RecordId(pid,i));
                markSlotUsed(i,true);
                return;
            }
        }
    }

    /**
     * 删除tuple
     * @param t
     * @throws DbException
     */
    public void deleteTuple(Tuple t) throws DbException {
        int tupleNumber = t.getRecordId().getTupleNumber();
        if(isSlotUsed(tupleNumber) && tuples[tupleNumber].equals(t) ){
            tuples[tupleNumber] = null;
            markSlotUsed(tupleNumber,false);
            return;
        }
        throw new DbException("tuple is not in tuples");
    }

    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len];
    }

    /**
     * 获取空tuple数量
     * @return
     */
    public int getNumEmptySlots() {
        int count = 0;
        for(int i=0;i<numSlots;i++){
            if(((header[i/8]>>(i%8))&1)==0){
                count++;
            }
        }
        return count;
    }


    /**
     * 计算每一页的tuple数量
     * @return
     */
    private int getNumTuples() {
        //每一个tuple还需要额外1bit的空间存储其状态
        return (int)Math.floor((BufferPool.getPageSize()*8.0)/(td.getSize()*8.0+1.0));

    }

    /**
     * 计算head大小
     * @return
     */
    private int getHeaderSize() {
        return (int)Math.ceil(getNumTuples()/8.0);
    }

    /**
     * 顺序读取tuple
     * @param dis 输入流
     * @param slotId tuple所在插槽id
     * @return
     * @throws NoSuchElementException
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        if (!isSlotUsed(slotId)) {
            //空tuple要将空bit读完
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * 从head中读取tuple状态
     * @param i
     * @return true 不为空 false 为空
     */
    public boolean isSlotUsed(int i) {
        int index = i/8;
        int offset = i%8;
        return ((header[index]>>offset)&1) == 1;
    }

    /**
     * 设置tuple状态
     * @param i
     * @param value
     */
    private void markSlotUsed(int i, boolean value) {
        int index = i/8;
        int offset = i%8;
        int tmp = 1<<(offset);
        byte b = header[index];
        if(value){
            header[index] =(byte) (b | tmp);
        }else{
            header[index] = (byte) (b & ~tmp);
        }
    }

    public Iterator<Tuple> iterator() {
        List<Tuple> tuples = new ArrayList<>();
        for(int i=0;i<numSlots;i++){
            if(isSlotUsed(i)){
                tuples.add(this.tuples[i]);
            }
        }
        return tuples.iterator();
    }

    @Override
    public PageId getId() {
        return this.pid;
    }

    @Override
    public TransactionId isDirty() {
        if(!dirty){
            return null;
        }
        return this.transactionId;
    }

    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        this.transactionId = tid;
    }

    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        //写入head
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //写入tuple数组
        for (int i = 0; i < tuples.length; i++) {
            //空tuple写入0
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }
            //非空tuple直接写入
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //末尾补0
        int zeroLen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length);
        byte[] zeroes = new byte[zeroLen];
        try {
            dos.write(zeroes, 0, zeroLen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //将缓冲区数据写入
        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }


}
