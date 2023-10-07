package index;

import common.DbException;
import common.Type;
import execution.Predicate;
import lombok.extern.slf4j.Slf4j;
import storage.*;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
public class BTreeLeafPage extends BTreePage{
    private final byte[] header;  //记录slot的占用情况
    private final Tuple[] tuples;  //存储该page的所有tuple
    private final int numSlots;  //叶节点中能存储的tuple数量（即n-1）

    // 页节点的双向链表结构
    private int leftSibling; //左兄弟的pageNumber，用于获取左兄弟的BTreePageId，为0则没有左兄弟
    private int rightSibling; //右兄弟的pageNumber，用于获取右兄弟的BTreePageId，为0则没有右兄弟

    public BTreeLeafPage(BTreePageId id, byte[] data, int key) throws IOException {
        super(id, key);
        this.numSlots = getMaxTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));


        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.parent = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.leftSibling = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.rightSibling = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }


        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try{

            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    public void checkRep(int fieldid, Field lowerBound, Field upperBound, boolean checkoccupancy, int depth) {
        Field prev = lowerBound;
        assert(this.getId().getPageCategory() == BTreePageId.LEAF);

        Iterator<Tuple> it = this.iterator();
        while (it.hasNext()) {
            Tuple t = it.next();
            assert(null == prev || prev.compare(Predicate.Op.LESS_THAN_OR_EQ, t.getField(fieldid)));
            prev = t.getField(fieldid);
            assert(t.getRecordId().getPageId().equals(this.getId()));
        }

        assert null == upperBound || null == prev || (prev.compare(Predicate.Op.LESS_THAN_OR_EQ, upperBound));

        assert !checkoccupancy || depth <= 0 || (getNumTuples() >= getMaxTuples() / 2);
    }

    public int getMaxTuples() {
        int bitsPerTupleIncludingHeader = td.getSize() * 8 + 1;
        int extraBits = 3 * INDEX_SIZE * 8;
        return (BufferPool.getPageSize()*8 - extraBits) / bitsPerTupleIncludingHeader;
    }

    private int getHeaderSize() {
        int tuplesPerPage = getMaxTuples();
        int hb = (tuplesPerPage / 8);
        if (hb * 8 < tuplesPerPage) hb++;

        return hb;
    }

    public BTreeLeafPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(this)
            {
                oldDataRef = oldData;
            }
            return new BTreeLeafPage(pid,oldDataRef,keyField);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
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

    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {

        if (!isSlotUsed(slotId)) {
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

    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            dos.writeInt(parent);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dos.writeInt(leftSibling);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dos.writeInt(rightSibling);

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

        for (int i=0; i<tuples.length; i++) {

            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length + 3 * INDEX_SIZE); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();
        if(rid == null)
            throw new DbException("tried to delete tuple with null rid");
        if((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
            throw new DbException("tried to delete tuple on invalid page or table");
        if (!isSlotUsed(rid.getTupleNumber()))
            throw new DbException("tried to delete null tuple.");
        markSlotUsed(rid.getTupleNumber(), false);
        t.setRecordId(null);
    }

    public void insertTuple(Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(td))
            throw new DbException("type mismatch, in addTuple");

        int emptySlot = -1;
        for (int i=0; i<numSlots; i++) {
            if (!isSlotUsed(i)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1)
            throw new DbException("called addTuple on page with no empty slots.");

        int lessOrEqKey = -1;
        Field key = t.getField(keyField);
        for (int i=0; i<numSlots; i++) {
            if(isSlotUsed(i)) {
                if(tuples[i].getField(keyField).compare(Predicate.Op.LESS_THAN_OR_EQ, key))
                    lessOrEqKey = i;
                else
                    break;
            }
        }

        int goodSlot = -1;
        if(emptySlot < lessOrEqKey) {
            for(int i = emptySlot; i < lessOrEqKey; i++) {
                moveRecord(i+1, i);
            }
            goodSlot = lessOrEqKey;
        }
        else {
            for(int i = emptySlot; i > lessOrEqKey + 1; i--) {
                moveRecord(i-1, i);
            }
            goodSlot = lessOrEqKey + 1;
        }

        // insert new record into the correct spot in sorted order
        markSlotUsed(goodSlot, true);
        log.info("BTreeLeafPage.insertTuple: new tuple, tableId = {} pageId = {} slotId = {}", pid.getTableId(), pid.getPageNumber(), goodSlot);
        RecordId rid = new RecordId(pid, goodSlot);
        t.setRecordId(rid);
        tuples[goodSlot] = t;
    }

    private void moveRecord(int from, int to) {
        if(!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            RecordId rid = new RecordId(pid, to);
            tuples[to] = tuples[from];
            tuples[to].setRecordId(rid);
            markSlotUsed(from, false);
        }
    }

    public BTreePageId getLeftSiblingId() {
        if(leftSibling == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), leftSibling, BTreePageId.LEAF);
    }

    public BTreePageId getRightSiblingId() {
        if(rightSibling == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), rightSibling, BTreePageId.LEAF);
    }

    public void setLeftSiblingId(BTreePageId id) throws DbException {
        if(id == null) {
            leftSibling = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setLeftSiblingId");
            }
            if(id.getPageCategory() != BTreePageId.LEAF) {
                throw new DbException("leftSibling must be a leaf node");
            }
            leftSibling = id.getPageNumber();
        }
    }

    public void setRightSiblingId(BTreePageId id) throws DbException {
        if(id == null) {
            rightSibling = 0;
        }
        else {
            if(id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setRightSiblingId");
            }
            if(id.getPageCategory() != BTreePageId.LEAF) {
                throw new DbException("rightSibling must be a leaf node");
            }
            rightSibling = id.getPageNumber();
        }
    }

    public int getNumTuples() {
        return numSlots - getNumEmptySlots();
    }

    public int getNumEmptySlots() {
        int cnt = 0;
        for(int i=0; i<numSlots; i++)
            if(!isSlotUsed(i))
                cnt++;
        return cnt;
    }


    public boolean isSlotUsed(int i) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;
        return (header[headerbyte] & (1 << headerbit)) != 0;
    }


    private void markSlotUsed(int i, boolean value) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;

        log.info("BTreeLeafPage.setSlot: setting slot {} to {}", i, value);
        if(value)
            header[headerbyte] |= 1 << headerbit;
        else
            header[headerbyte] &= (0xFF ^ (1 << headerbit));
    }


    public Iterator<Tuple> iterator() {
        return new BTreeLeafPageIterator(this);
    }


    public Iterator<Tuple> reverseIterator() {
        return new BTreeLeafPageReverseIterator(this);
    }

    Tuple getTuple(int i) throws NoSuchElementException {

        if (i >= tuples.length)
            throw new NoSuchElementException();

        try {
            if(!isSlotUsed(i)) {
                log.info("BTreeLeafPage.getTuple: slot {} in {}:{} is not used", i, pid.getTableId(), pid.getPageNumber());
                return null;
            }

            log.info("BTreeLeafPage.getTuple: returning tuple {}", i);
            return tuples[i];

        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }


}
class BTreeLeafPageIterator implements Iterator<Tuple> {
    int curTuple = 0;
    Tuple nextToReturn = null;
    final BTreeLeafPage p;

    public BTreeLeafPageIterator(BTreeLeafPage p) {
        this.p = p;
    }

    public boolean hasNext() {
        if (nextToReturn != null)
            return true;

        try {
            while (true) {
                nextToReturn = p.getTuple(curTuple++);
                if(nextToReturn != null)
                    return true;
            }
        } catch(NoSuchElementException e) {
            return false;
        }
    }

    public Tuple next() {
        Tuple next = nextToReturn;

        if (next == null) {
            if (hasNext()) {
                next = nextToReturn;
                nextToReturn = null;
                return next;
            } else
                throw new NoSuchElementException();
        } else {
            nextToReturn = null;
            return next;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}


class BTreeLeafPageReverseIterator implements Iterator<Tuple> {
    int curTuple;
    Tuple nextToReturn = null;
    final BTreeLeafPage p;

    public BTreeLeafPageReverseIterator(BTreeLeafPage p) {
        this.p = p;
        this.curTuple = p.getMaxTuples() - 1;
    }

    public boolean hasNext() {
        if (nextToReturn != null)
            return true;

        try {
            while (curTuple >= 0) {
                nextToReturn = p.getTuple(curTuple--);
                if(nextToReturn != null)
                    return true;
            }
            return false;
        } catch(NoSuchElementException e) {
            return false;
        }
    }

    public Tuple next() {
        Tuple next = nextToReturn;

        if (next == null) {
            if (hasNext()) {
                next = nextToReturn;
                nextToReturn = null;
                return next;
            } else
                throw new NoSuchElementException();
        } else {
            nextToReturn = null;
            return next;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

