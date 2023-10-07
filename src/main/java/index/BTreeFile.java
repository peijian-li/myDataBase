package index;

import common.Database;
import common.DbException;
import common.Permissions;
import common.TransactionAbortedException;
import execution.IndexPredicate;
import execution.Predicate;
import lombok.extern.slf4j.Slf4j;
import storage.*;
import transaction.TransactionId;

import java.io.*;
import java.util.*;


@Slf4j
public class BTreeFile implements DbFile {
    private final File f;
    private final TupleDesc td;
    private final int tableid;
    private final int keyField;//主键字段id

    public BTreeFile(File f, int key, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.keyField = key;
        this.td = td;
    }


    @Override
    public Page readPage(PageId pid) {
        BTreePageId id = (BTreePageId) pid;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            if (id.getPageCategory() == BTreePageId.ROOT_PTR) {
                //指向根节点指针
                byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read " + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                log.info("BTreeFile.readPage: read page {}", id.getPageNumber());
                return new BTreeRootPtrPage(id, pageBuf);
            } else {
                byte[] pageBuf = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) != BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read " + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                log.info("BTreeFile.readPage: read page {}", id.getPageNumber());
                //分别是三种节点
                if (id.getPageCategory() == BTreePageId.INTERNAL) {
                    return new BTreeInternalPage(id, pageBuf, keyField);
                } else if (id.getPageCategory() == BTreePageId.LEAF) {
                    return new BTreeLeafPage(id, pageBuf, keyField);
                } else {
                    return new BTreeHeaderPage(id, pageBuf);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writePage(Page page) throws IOException {
        BTreePageId id = (BTreePageId) page.getId();
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        if(id.getPageCategory() == BTreePageId.ROOT_PTR) {
            rf.write(data);
            rf.close();
        }
        else {
            rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId rootId = rootPtr.getRootId();

        if(rootId == null) {
            rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            rootPtr.setRootId(rootId);
        }

        //寻找插入的叶节点
        BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
        //叶节点没有空位就分裂
        if(leafPage.getNumEmptySlots() == 0) {
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
        }

        leafPage.insertTuple(t);

        return new ArrayList<>(dirtypages.values());
    }

    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(), BTreePageId.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        //空位超过一半
        int maxEmptySlots =  (page.getMaxTuples()+1)/2;
        if(page.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, page);
        }

        return new ArrayList<>(dirtypages.values());
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new BTreeFileIterator(this, tid);
    }

    public DbFileIterator Iterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

    @Override
    public int getId() {
        return tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    public int numPages() {
        //只有完整的页
        return (int) ((f.length() - BTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
    }

    public int keyField() {
        return keyField;
    }

    public BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Field f) throws DbException, TransactionAbortedException {
        return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
    }

    /**
     * 寻找主键f所在叶节点
     * @param tid
     * @param dirtypages
     * @param pid
     * @param perm
     * @param f
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm, Field f) throws DbException, TransactionAbortedException {
        //1. 如果是叶子节点，直接返回
        if(pid.getPageCategory() == BTreePageId.LEAF){
            return (BTreeLeafPage) getPage(tid,dirtypages,pid,perm);
        }
        BTreeInternalPage page = (BTreeInternalPage) getPage(tid,dirtypages,pid,perm);
        Iterator<BTreeEntry> iterator = page.iterator();
        //2. 如果filed为空，找到最左边的节点
        if(f==null){
            if(iterator.hasNext()){
                return findLeafPage(tid,dirtypages,iterator.next().getLeftChild(),perm,f);
            }
            return null;
        }

        BTreeEntry next = null;
        //3. 否则，内部节点查找符合条件的entry，并递归查找
        while(iterator.hasNext()){
            next =  iterator.next();
            Field key = next.getKey();
            //当有重复值的时候 节点分裂有可能一半在左边一半在右边，所以是小于等于
            if(f.compare(Predicate.Op.LESS_THAN_OR_EQ,key)){
                return findLeafPage(tid,dirtypages,next.getLeftChild(),perm,f);
            }
        }
        //最后一个entry的右子节点
        if(next!=null){
            return findLeafPage(tid,dirtypages,next.getRightChild(),perm,f);
        }
        return null;
    }

    /**
     * 分裂叶子节点
     * @param tid
     * @param dirtypages
     * @param page
     * @param field
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        //1. 将当前page的后半部分放入新page
        int half = page.getNumTuples()/2;
        BTreeLeafPage newPage = (BTreeLeafPage)getEmptyPage(tid,dirtypages,BTreePageId.LEAF);
        Iterator<Tuple> iterator = page.reverseIterator();
        while(iterator.hasNext() && half>0){
            Tuple next = iterator.next();
            //从旧页中删除，插入新页
            page.deleteTuple(next);
            newPage.insertTuple(next);
            half--;
        }
        //2. 将中间tuple插入父节点
        Tuple up = iterator.next();
        BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), field);
        BTreeEntry insertEntry = new BTreeEntry(up.getField(keyField), page.getId(), newPage.getId());
        parentPage.insertEntry(insertEntry);
        //3. 设置节点间的关系
        if(page.getRightSiblingId()!=null){
            BTreeLeafPage right = (BTreeLeafPage) getPage(tid, dirtypages, page.getRightSiblingId(), Permissions.READ_WRITE);
            right.setLeftSiblingId(newPage.getId());
            dirtypages.put(right.getId(),right);
        }
        newPage.setRightSiblingId(page.getRightSiblingId());
        newPage.setLeftSiblingId(page.getId());
        page.setRightSiblingId(newPage.getId());

        page.setParentId(parentPage.getId());
        newPage.setParentId(parentPage.getId());

        //4. 增加脏页
        dirtypages.put(parentPage.getId(),parentPage);
        dirtypages.put(page.getId(),page);
        dirtypages.put(newPage.getId(),newPage);

        //5. 返回要插入field的页
        if (field.compare(Predicate.Op.GREATER_THAN_OR_EQ, up.getField(keyField))) {
            return newPage;
        }
        return page;
    }

    /**
     * 获取有空位的父节点，无空位则分裂父节点
     * @param tid
     * @param dirtypages
     * @param parentId
     * @param field
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId parentId, Field field)
            throws DbException, IOException, TransactionAbortedException {
        BTreeInternalPage parent;
        if(parentId.getPageCategory() == BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId();
            rootPtr.setRootId(parent.getId());
            BTreePage prevRootPage = (BTreePage)getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        }
        else {
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
        }

        //分裂父节点
        if(parent.getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }
        return parent;

    }

    /**
     * 分裂内部节点
     * @param tid
     * @param dirtypages
     * @param page
     * @param field
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    private BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        //1. 将当前page的后半部分放入新page
        int half = page.getNumEntries()/2;
        BTreeInternalPage newPage = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        Iterator<BTreeEntry> iterator = page.reverseIterator();
        while(iterator.hasNext() && half>0){
            BTreeEntry next = iterator.next();
            page.deleteKeyAndRightChild(next);
            newPage.insertEntry(next);
            half--;
        }
        //2. 将中间entry插入父节点，并从原来的节点删除
        BTreeEntry up = iterator.next();
        page.deleteKeyAndRightChild(up);
        up.setLeftChild(page.getId());
        up.setRightChild(newPage.getId());
        BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), field);
        parentPage.insertEntry(up);
        page.setParentId(parentPage.getId());
        newPage.setParentId(parentPage.getId());

        //3. 设置newPage子节点的父节点指向
        updateParentPointers(tid,dirtypages,newPage);

        //4. 增加脏页
        dirtypages.put(parentPage.getId(),parentPage);
        dirtypages.put(newPage.getId(),newPage);
        dirtypages.put(page.getId(),page);
        //5. 返回要插入field的页
        if (field.compare(Predicate.Op.GREATER_THAN_OR_EQ, up.getKey())) {
            return newPage;
        }
        return page;
    }

    /**
     * 将page的子节点的父节点指向page
     * @param tid
     * @param dirtypages
     * @param page
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page)
            throws DbException, TransactionAbortedException{
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while(it.hasNext()) {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if(e != null) {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }


    private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtyPages, BTreePageId pid, BTreePageId child)
            throws DbException, TransactionAbortedException {
        BTreePage p = (BTreePage) getPage(tid, dirtyPages, child, Permissions.READ_ONLY);
        //child的父节点不是page则修改指向
        if(!p.getParentId().equals(pid)) {
            p = (BTreePage) getPage(tid, dirtyPages, child, Permissions.READ_WRITE);
            p.setParentId(pid);
        }
    }

    /**
     * 通过pageId获取page，并把page放入脏页map
     * @param tid
     * @param dirtypages
     * @param pid
     * @param perm
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
            throws DbException, TransactionAbortedException {
        if(dirtypages.containsKey(pid)) {
            return dirtypages.get(pid);
        }
        else {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if(perm == Permissions.READ_WRITE) {
                dirtypages.put(pid, p);
            }
            return p;
        }
    }

    /**
     * 处理空位超过一半的page
     * @param tid
     * @param dirtypages
     * @param page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId parentId = page.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;
        //不是根节点，找到左右父entry
        if(parentId.getPageCategory() != BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
            Iterator<BTreeEntry> ite = parent.iterator();
            while(ite.hasNext()) {
                BTreeEntry e = ite.next();
                if(e.getLeftChild().equals(page.getId())) {
                    rightEntry = e;
                    break;
                }
                else if(e.getRightChild().equals(page.getId())) {
                    leftEntry = e;
                }
            }
        }

        if(page.getId().getPageCategory() == BTreePageId.LEAF) {
            handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
        }
        else {
            handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
        }
    }

    private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page,
                                            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        //获得左右兄弟
        if(leftEntry != null)
            leftSiblingId = leftEntry.getLeftChild();
        if(rightEntry != null)
            rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2;
        if(leftSiblingId != null) {
            //左兄弟空位超过一半，则与左兄弟合并，否则从左兄弟窃取
            BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        }
        else if(rightSiblingId != null) {
            //右兄弟空位超过一半，则与右兄弟合并，否则从右兄弟窃取
            BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                                BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if(leftEntry != null)
            leftSiblingId = leftEntry.getLeftChild();
        if(rightEntry != null)
            rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries()/2; // ceiling
        if(leftSiblingId != null) {
            //左兄弟空位超过一半，则与左兄弟合并，否则从左兄弟窃取
            BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        }
        else if(rightSiblingId != null) {
            //右兄弟空位超过一半，则与右兄弟合并，否则从右兄弟窃取
            BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }

    private void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling, BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
        //1. 获取要窃取的tuple个数，平均数量-原来数量
        int stealNum = ((page.getNumTuples()+sibling.getNumTuples())/2) - page.getNumTuples();
        if(stealNum<=0){
            return;
        }
        //2. 判断是从左还是右节点窃取，并获取迭代器
        Iterator<Tuple> tupleIterator;
        if(isRightSibling){
            tupleIterator = sibling.iterator();
        }else{
            tupleIterator = sibling.reverseIterator();
        }
        //3. 进行窃取
        Tuple next = null;
        while(stealNum>0){
            next = tupleIterator.next();
            sibling.deleteTuple(next);
            page.insertTuple(next);
            stealNum--;
        }
        //4. 新建一个entry插入父节点
        if(isRightSibling){
            entry.setKey(tupleIterator.next().getField(keyField));
        }else{
            entry.setKey(next.getField(keyField));
        }
        parent.updateEntry(entry);
    }

    private void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, TransactionAbortedException {
        //1. 获取要窃取的tuple个数，平均数量-原来数量
        int stealNum = (page.getNumEntries()+leftSibling.getNumEntries())/2 - page.getNumEntries();
        if(stealNum<=0){
            return;
        }
        //2. 获取迭代器和节点
        Iterator<BTreeEntry> leftIterator = leftSibling.reverseIterator();
        Iterator<BTreeEntry> pageIterator = page.iterator();
        BTreeEntry leftLastEntry = leftIterator.next();
        BTreeEntry pageFirstEntry = pageIterator.next();
        //3. 先将父节点中的中间entry插入page
        BTreeEntry midEntry = new BTreeEntry(parentEntry.getKey(), leftLastEntry.getRightChild(), pageFirstEntry.getLeftChild());
        page.insertEntry(midEntry);
        stealNum--;

        //4. 再插入左节点中的entry
        while(stealNum>0){
            leftSibling.deleteKeyAndRightChild(leftLastEntry);
            page.insertEntry(leftLastEntry);
            stealNum--;
            leftLastEntry = leftIterator.next();
        }

        //5. 将左节点最大的entry插入到父节点
        leftSibling.deleteKeyAndRightChild(leftLastEntry);
        parentEntry.setKey(leftLastEntry.getKey());
        parent.updateEntry(parentEntry);

        //6. 设置newPage子节点的父节点指向
        updateParentPointers(tid,dirtypages,page);

        //7. 增加脏页
        dirtypages.put(leftSibling.getId(),leftSibling);
        dirtypages.put(page.getId(),page);
        dirtypages.put(parent.getId(),parent);
    }

    private void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, TransactionAbortedException {
        //1. 获取要窃取的tuple个数，平均数量-原来数量
        int stealNum = (page.getNumEntries()+rightSibling.getNumEntries())/2 - page.getNumEntries();
        if(stealNum<=0){
            return;
        }
        //2. 获取迭代器和节点
        Iterator<BTreeEntry> rightIterator = rightSibling.iterator();
        Iterator<BTreeEntry> pageIterator = page.reverseIterator();
        BTreeEntry rightFirstEntry = rightIterator.next();
        BTreeEntry pageLastEntry = pageIterator.next();
        //3. 先将父节点中的中间entry插入page
        BTreeEntry midEntry = new BTreeEntry(parentEntry.getKey(), pageLastEntry.getRightChild(), rightFirstEntry.getLeftChild());
        page.insertEntry(midEntry);
        stealNum--;

        //4. 再插入右节点中的entry
        while(stealNum>0){
            rightSibling.deleteKeyAndRightChild(rightFirstEntry);
            page.insertEntry(pageLastEntry);
            stealNum--;
            rightFirstEntry = rightIterator.next();
        }

        //5. 将右节点最小的entry插入到父节点
        rightSibling.deleteKeyAndRightChild(rightFirstEntry);
        parentEntry.setKey(rightFirstEntry.getKey());
        parent.updateEntry(parentEntry);

        //6. 设置newPage子节点的父节点指向
        updateParentPointers(tid,dirtypages,page);

        //7. 增加脏页
        dirtypages.put(rightSibling.getId(),rightSibling);
        dirtypages.put(page.getId(),page);
        dirtypages.put(parent.getId(),parent);
    }

    private void mergeLeafPages(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {
        //1. 设置右兄弟节点
        leftPage.setRightSiblingId(rightPage.getRightSiblingId());
        //2. 如果右节点有右兄弟节点，就设置其左节点指向
        if(rightPage.getRightSiblingId()!=null){
            BTreeLeafPage rightSiblingPage= (BTreeLeafPage) getPage(tid, dirtypages, rightPage.getRightSiblingId(), Permissions.READ_WRITE);
            rightSiblingPage.setLeftSiblingId(leftPage.getId());
            dirtypages.put(rightSiblingPage.getId(),rightSiblingPage);
        }
        //3. 进行合并
        Iterator<Tuple> iterator = rightPage.iterator();
        while(iterator.hasNext()){
            Tuple next = iterator.next();
            rightPage.deleteTuple(next);
            leftPage.insertTuple(next);
        }
        //4. 设置空page并删除父结点的entry
        setEmptyPage(tid,dirtypages,rightPage.getId().getPageNumber());
        deleteParentEntry(tid,dirtypages,leftPage,parent,parentEntry);
        //5. 增加脏页
        dirtypages.remove(rightPage.getId());
        dirtypages.put(leftPage.getId(),leftPage);
        dirtypages.put(parent.getId(),parent);
    }

    private void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {
        //1. 获取两个节点的迭代器
        Iterator<BTreeEntry> leftIterator = leftPage.reverseIterator();
        Iterator<BTreeEntry> rightIterator = rightPage.iterator();
        BTreeEntry leftLastEntry = leftIterator.next();
        BTreeEntry rightFirstEntry = rightIterator.next();
        //2. 将两节点中间的父节点entry插入到左节点，并将其删除
        BTreeEntry midEntry = new BTreeEntry(parentEntry.getKey(), leftLastEntry.getRightChild(), rightFirstEntry.getLeftChild());
        leftPage.insertEntry(midEntry);
        deleteParentEntry(tid,dirtypages,leftPage,parent,parentEntry);
        //3. 插入右节点的第一个entry
        rightPage.deleteKeyAndLeftChild(rightFirstEntry);
        leftPage.insertEntry(rightFirstEntry);
        //4. 循环插入右节点的entry
        while(rightIterator.hasNext()){
            rightFirstEntry = rightIterator.next();
            rightPage.deleteKeyAndLeftChild(rightFirstEntry);
            leftPage.insertEntry(rightFirstEntry);
        }
        //5. 更新左节点的子节点的父节点指向
        updateParentPointers(tid,dirtypages,leftPage);
        //6. 设置空page
        setEmptyPage(tid,dirtypages,rightPage.getId().getPageNumber());
        //7. 增加脏页
        dirtypages.remove(rightPage.getId());
        dirtypages.put(leftPage.getId(),leftPage);
        dirtypages.put(parent.getId(),parent);
    }

    private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {
        parent.deleteKeyAndRightChild(parentEntry);
        int maxEmptySlots = (parent.getMaxEntries() +1)/2;
        if(parent.getNumEmptySlots() == parent.getMaxEntries()) {
            //parent全部为空，说明parent是根节点（parent无兄弟节点，在空位超过一半时不处理），删除根节点，左节点成为新的根节点
            BTreePageId rootPtrId = parent.getParentId();
            if(rootPtrId.getPageCategory() != BTreePageId.ROOT_PTR) {
                throw new DbException("attempting to delete a non-root node");
            }
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
            leftPage.setParentId(rootPtrId);
            rootPtr.setRootId(leftPage.getId());
            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        } else if(parent.getNumEmptySlots() > maxEmptySlots) {
            //parent空位超过一半
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }

    private BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        synchronized(this) {
            if(f.length() == 0) {
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyRootPtrData);
                bw.write(emptyLeafData);
                bw.close();
            }
        }
        return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
    }

    private int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages)
            throws DbException, IOException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if(headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            while(headerPage != null && headerPage.getEmptySlot() == -1) {
                headerId = headerPage.getNextPageId();
                if(headerId != null) {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                }
                else {
                    headerPage = null;
                }
            }


            if(headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        if(headerId == null) {
            synchronized(this) {
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }

    private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pageCategory)
            throws DbException, IOException, TransactionAbortedException {
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pageCategory);

        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        Database.getBufferPool().discardPage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }

    private void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo)
            throws DbException, IOException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        BTreePageId prevId = null;
        int headerPageCount = 0;

        //如果第一个headerPage为空，新建headerPage
        if(headerId == null) {
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            rootPtr.setHeaderId(headerId);
        }

        //遍历链表，计算emptyPageNo所在headerPage
        while(headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            prevId = headerId;
            headerId = headerPage.getNextPageId();
            headerPageCount++;
        }

        //如果遍历到emptyPageNo所在headerPage前为空，新建headerPage添加到链表末尾
        while((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            headerPage.setPrevPageId(prevId);
            prevPage.setNextPageId(headerId);
            headerPageCount++;
            prevId = headerId;
        }

        //将emptyPageNo插槽设置为空
        BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
        headerPage.markSlotUsed(emptySlot, false);
    }


}
class BTreeFileIterator implements DbFileIterator {
    private Tuple next = null;
    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;


    public BTreeFileIterator(BTreeFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }


    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        curp = f.findLeafPage(tid, root, null);
        it = curp.iterator();
    }


    private Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curp != null) {
            BTreePageId nextp = curp.getRightSiblingId();
            if(nextp == null) {
                curp = null;
            }
            else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext())
                    it = null;
            }
        }

        if (it == null)
            return null;
        return it.next();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }



    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }


    public void close() {
        next = null;
        it = null;
        curp = null;
    }
}


class BTreeSearchIterator implements DbFileIterator {
    private Tuple next = null;
    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;
    final IndexPredicate ipred;


    public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
        this.f = f;
        this.tid = tid;
        this.ipred = ipred;
    }


    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if(ipred.getOp() == Predicate.Op.EQUALS || ipred.getOp() == Predicate.Op.GREATER_THAN || ipred.getOp() == Predicate.Op.GREATER_THAN_OR_EQ) {
            curp = f.findLeafPage(tid, root, ipred.getField());
        }
        else {
            curp = f.findLeafPage(tid, root, null);
        }
        it = curp.iterator();
    }


    public Tuple readNext() throws TransactionAbortedException, DbException, NoSuchElementException {
        while (it != null) {
            while (it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
                    return t;
                }
                else if(ipred.getOp() == Predicate.Op.LESS_THAN || ipred.getOp() == Predicate.Op.LESS_THAN_OR_EQ) {
                    return null;
                }
                else if(ipred.getOp() == Predicate.Op.EQUALS && t.getField(f.keyField()).compare(Predicate.Op.GREATER_THAN, ipred.getField())) {
                    return null;
                }
            }

            BTreePageId nextp = curp.getRightSiblingId();
            if(nextp == null) {
                return null;
            }
            else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
            }
        }

        return null;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }


    public void close() {
        next=null;
        it = null;
    }
}
