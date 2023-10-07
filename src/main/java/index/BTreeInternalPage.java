package index;

import common.DbException;
import common.Type;
import execution.Predicate;
import lombok.extern.slf4j.Slf4j;
import storage.BufferPool;
import storage.Field;
import storage.IntField;
import storage.RecordId;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
public class BTreeInternalPage extends BTreePage{
    private final byte[] header; //记录slot的占用情况
    private final Field[] keys;  //存储key的数组
    private final int[] children;  //存储page的序号，用于获取左孩子、右孩子的BTreePageId
    private final int numSlots;  //不是key的数量，而是内部节点中能存储的指针的数量（即n，内部节点中最多能存储key的数量为n-1）

    private int childCategory;  //孩子节点的类型（内部节点或叶节点）

    public BTreeInternalPage(BTreePageId id, byte[] data, int key) throws IOException {
        super(id, key);
        this.numSlots = getMaxEntries() + 1;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.parent = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        childCategory = dis.readByte();

        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        keys = new Field[numSlots];
        try{
            keys[0] = null;
            for (int i=1; i<keys.length; i++)
                keys[i] = readNextKey(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }

        children = new int[numSlots];
        try{
            for (int i=0; i<children.length; i++)
                children[i] = readNextChild(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

    }

    public int getMaxEntries() {
        int keySize = td.getFieldType(keyField).getLen();
        int bitsPerEntryIncludingHeader = keySize * 8 + INDEX_SIZE * 8 + 1;
        int extraBits = 2 * INDEX_SIZE * 8 + 8 + 1;
        return (BufferPool.getPageSize()*8 - extraBits) / bitsPerEntryIncludingHeader;
    }

    private int getHeaderSize() {
        int slotsPerPage = getMaxEntries() + 1;
        int hb = (slotsPerPage / 8);
        if (hb * 8 < slotsPerPage) hb++;

        return hb;
    }

    private Field readNextKey(DataInputStream dis, int slotId) throws NoSuchElementException {
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getFieldType(keyField).getLen(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty key");
                }
            }
            return null;
        }

        Field f;
        try {
            f = td.getFieldType(keyField).parse(dis);
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return f;
    }

    private int readNextChild(DataInputStream dis, int slotId) throws NoSuchElementException {
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<INDEX_SIZE; i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty child pointer");
                }
            }
            return -1;
        }

        int child = -1;
        try {
            Field f = Type.INT_TYPE.parse(dis);
            child = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return child;
    }

    public void checkRep(Field lowerBound, Field upperBound, boolean checkOccupancy, int depth) {
        Field prev = lowerBound;
        assert(this.getId().getPageCategory() == BTreePageId.INTERNAL);

        Iterator<BTreeEntry> it  = this.iterator();
        while (it.hasNext()) {
            Field f = it.next().getKey();
            assert(null == prev || prev.compare(Predicate.Op.LESS_THAN_OR_EQ,f));
            prev = f;
        }

        assert null == upperBound || null == prev || (prev.compare(Predicate.Op.LESS_THAN_OR_EQ, upperBound));

        assert !checkOccupancy || depth <= 0 || (getNumEntries() >= getMaxEntries() / 2);
    }

    public int getNumEntries() {
        return numSlots - getNumEmptySlots() - 1;
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
            dos.writeByte((byte) childCategory);

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

        for (int i=1; i<keys.length; i++) {

            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getFieldType(keyField).getLen(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }


            try {
                keys[i].serialize(dos);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        for (int i=0; i<children.length; i++) {

            if (!isSlotUsed(i)) {
                for (int j=0; j<INDEX_SIZE; j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            try {
                dos.writeInt(children[i]);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        int zerolen = BufferPool.getPageSize() - (INDEX_SIZE + 1 + header.length +
                td.getFieldType(keyField).getLen() * (keys.length - 1) + INDEX_SIZE * children.length);
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

    public int getNumEmptySlots() {
        int cnt = 0;
        for(int i=1; i<numSlots; i++)
            if(!isSlotUsed(i))
                cnt++;
        return cnt;
    }


    public boolean isSlotUsed(int i) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;
        return (header[headerbyte] & (1 << headerbit)) != 0;
    }

    public void deleteKeyAndRightChild(BTreeEntry e) throws DbException {
        deleteEntry(e, true);
    }


    public void deleteKeyAndLeftChild(BTreeEntry e) throws DbException {
        deleteEntry(e, false);
    }

    private void deleteEntry(BTreeEntry e, boolean deleteRightChild) throws DbException {
        RecordId rid = e.getRecordId();
        if(rid == null)
            throw new DbException("tried to delete entry with null rid");
        if((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
            throw new DbException("tried to delete entry on invalid page or table");
        if (!isSlotUsed(rid.getTupleNumber()))
            throw new DbException("tried to delete null entry.");
        if(deleteRightChild) {
            markSlotUsed(rid.getTupleNumber(), false);
        }
        else {
            for(int i = rid.getTupleNumber() - 1; i >= 0; i--) {
                if(isSlotUsed(i)) {
                    children[i] = children[rid.getTupleNumber()];
                    markSlotUsed(rid.getTupleNumber(), false);
                    break;
                }
            }
        }
        e.setRecordId(null);
    }

    public void updateEntry(BTreeEntry e) throws DbException {
        RecordId rid = e.getRecordId();
        if(rid == null)
            throw new DbException("tried to update entry with null rid");
        if((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
            throw new DbException("tried to update entry on invalid page or table");
        if (!isSlotUsed(rid.getTupleNumber()))
            throw new DbException("tried to update null entry.");

        for(int i = rid.getTupleNumber() + 1; i < numSlots; i++) {
            if(isSlotUsed(i)) {
                if(keys[i].compare(Predicate.Op.LESS_THAN, e.getKey())) {
                    throw new DbException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be less than or equal to keys on the right");
                }
                break;
            }
        }
        for(int i = rid.getTupleNumber() - 1; i >= 0; i--) {
            if(isSlotUsed(i)) {
                if(i > 0 && keys[i].compare(Predicate.Op.GREATER_THAN, e.getKey())) {
                    throw new DbException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be greater than or equal to keys on the left");
                }
                children[i] = e.getLeftChild().getPageNumber();
                break;
            }
        }
        children[rid.getTupleNumber()] = e.getRightChild().getPageNumber();
        keys[rid.getTupleNumber()] = e.getKey();
    }

    public void insertEntry(BTreeEntry e) throws DbException {
        if (!e.getKey().getType().equals(td.getFieldType(keyField)))
            throw new DbException("key field type mismatch, in insertEntry");

        if(e.getLeftChild().getTableId() != pid.getTableId() || e.getRightChild().getTableId() != pid.getTableId())
            throw new DbException("table id mismatch in insertEntry");

        if(childCategory == 0) {
            if(e.getLeftChild().getPageCategory() != e.getRightChild().getPageCategory())
                throw new DbException("child page category mismatch in insertEntry");

            childCategory = e.getLeftChild().getPageCategory();
        }
        else if(e.getLeftChild().getPageCategory() != childCategory || e.getRightChild().getPageCategory() != childCategory)
            throw new DbException("child page category mismatch in insertEntry");

        if(getNumEmptySlots() == getMaxEntries()) {
            children[0] = e.getLeftChild().getPageNumber();
            children[1] = e.getRightChild().getPageNumber();
            keys[1] = e.getKey();
            markSlotUsed(0, true);
            markSlotUsed(1, true);
            e.setRecordId(new RecordId(pid, 1));
            return;
        }

        int emptySlot = -1;
        for (int i=1; i<numSlots; i++) {
            if (!isSlotUsed(i)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1)
            throw new DbException("called insertEntry on page with no empty slots.");

        int lessOrEqKey = -1;
        for (int i=0; i<numSlots; i++) {
            if(isSlotUsed(i)) {
                if(children[i] == e.getLeftChild().getPageNumber() || children[i] == e.getRightChild().getPageNumber()) {
                    if(i > 0 && keys[i].compare(Predicate.Op.GREATER_THAN, e.getKey())) {
                        throw new DbException("attempt to insert invalid entry with left child " +
                                e.getLeftChild().getPageNumber() + ", right child " +
                                e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    lessOrEqKey = i;
                    if(children[i] == e.getRightChild().getPageNumber()) {
                        children[i] = e.getLeftChild().getPageNumber();
                    }
                }
                else if(lessOrEqKey != -1) {
                    // validate that the next key is greater than or equal to the one we are inserting
                    if(keys[i].compare(Predicate.Op.LESS_THAN, e.getKey())) {
                        throw new DbException("attempt to insert invalid entry with left child " +
                                e.getLeftChild().getPageNumber() + ", right child " +
                                e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    break;
                }
            }
        }

        if(lessOrEqKey == -1) {
            throw new DbException("attempt to insert invalid entry with left child " +
                    e.getLeftChild().getPageNumber() + ", right child " +
                    e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                    " HINT: one of these children must match an existing child on the page" +
                    " and this key must be correctly ordered in between that child's" +
                    " left and right keys");
        }

        int goodSlot = -1;
        if(emptySlot < lessOrEqKey) {
            for(int i = emptySlot; i < lessOrEqKey; i++) {
                moveEntry(i+1, i);
            }
            goodSlot = lessOrEqKey;
        }
        else {
            for(int i = emptySlot; i > lessOrEqKey + 1; i--) {
                moveEntry(i-1, i);
            }
            goodSlot = lessOrEqKey + 1;
        }

        markSlotUsed(goodSlot, true);
        log.info( "BTreeLeafPage.insertEntry: new entry, tableId = {} pageId = {} slotId = {}", pid.getTableId(), pid.getPageNumber(), goodSlot);
        keys[goodSlot] = e.getKey();
        children[goodSlot] = e.getRightChild().getPageNumber();
        e.setRecordId(new RecordId(pid, goodSlot));
    }

    private void moveEntry(int from, int to) {
        if(!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            keys[to] = keys[from];
            children[to] = children[from];
            markSlotUsed(from, false);
        }
    }

    private void markSlotUsed(int i, boolean value) {
        int headerbit = i % 8;
        int headerbyte = (i - headerbit) / 8;

        log.info( "BTreeInternalPage.setSlot: setting slot {} to {}", i, value);
        if(value)
            header[headerbyte] |= 1 << headerbit;
        else
            header[headerbyte] &= (0xFF ^ (1 << headerbit));
    }


    public Iterator<BTreeEntry> iterator() {
        return new BTreeInternalPageIterator(this);
    }

    public Iterator<BTreeEntry> reverseIterator() {
        return new BTreeInternalPageReverseIterator(this);
    }

    protected Field getKey(int i) throws NoSuchElementException {

        if (i <= 0 || i >= keys.length)
            throw new NoSuchElementException();

        try {
            if(!isSlotUsed(i)) {
                log.info("BTreeInternalPage.getKey: slot {} in {}:{} is not used", i, pid.getTableId(), pid.getPageNumber());
                return null;
            }

            log.info( "BTreeInternalPage.getKey: returning key {}", i);
            return keys[i];

        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }


    protected BTreePageId getChildId(int i) throws NoSuchElementException {

        if (i < 0 || i >= children.length)
            throw new NoSuchElementException();

        try {
            if(!isSlotUsed(i)) {
                log.info("BTreeInternalPage.getChildId: slot {} in {}:{} is not used", i, pid.getTableId(), pid.getPageNumber());
                return null;
            }

            log.info("BTreeInternalPage.getChildId: returning child id {}", i);
            return new BTreePageId(pid.getTableId(), children[i], childCategory);

        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }


}
class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
    int curEntry = 1;
    BTreePageId prevChildId = null;
    BTreeEntry nextToReturn = null;
    final BTreeInternalPage p;

    public BTreeInternalPageIterator(BTreeInternalPage p) {
        this.p = p;
    }

    public boolean hasNext() {
        if (nextToReturn != null)
            return true;

        try {
            if(prevChildId == null) {
                prevChildId = p.getChildId(0);
                if(prevChildId == null) {
                    return false;
                }
            }
            while (true) {
                int entry = curEntry++;
                Field key = p.getKey(entry);
                BTreePageId childId = p.getChildId(entry);
                if(key != null && childId != null) {
                    nextToReturn = new BTreeEntry(key, prevChildId, childId);
                    nextToReturn.setRecordId(new RecordId(p.pid, entry));
                    prevChildId = childId;
                    return true;
                }
            }
        } catch(NoSuchElementException e) {
            return false;
        }
    }

    public BTreeEntry next() {
        BTreeEntry next = nextToReturn;

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
class BTreeInternalPageReverseIterator implements Iterator<BTreeEntry> {
    int curEntry;
    BTreePageId nextChildId = null;
    BTreeEntry nextToReturn = null;
    final BTreeInternalPage p;

    public BTreeInternalPageReverseIterator(BTreeInternalPage p) {
        this.p = p;
        this.curEntry = p.getMaxEntries();
        while(!p.isSlotUsed(curEntry) && curEntry > 0) {
            --curEntry;
        }
    }

    public boolean hasNext() {
        if (nextToReturn != null)
            return true;

        try {
            if(nextChildId == null) {
                nextChildId = p.getChildId(curEntry);
                if(nextChildId == null) {
                    return false;
                }
            }
            while (true) {
                int entry = curEntry--;
                Field key = p.getKey(entry);
                BTreePageId childId = p.getChildId(entry - 1);
                if(key != null && childId != null) {
                    nextToReturn = new BTreeEntry(key, childId, nextChildId);
                    nextToReturn.setRecordId(new RecordId(p.pid, entry));
                    nextChildId = childId;
                    return true;
                }
            }
        } catch(NoSuchElementException e) {
            return false;
        }
    }

    public BTreeEntry next() {
        BTreeEntry next = nextToReturn;

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
