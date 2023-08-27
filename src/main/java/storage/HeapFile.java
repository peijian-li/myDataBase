package storage;

import common.Database;
import common.DbException;
import common.Permissions;
import common.TransactionAbortedException;
import lombok.AllArgsConstructor;
import transaction.TransactionId;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@AllArgsConstructor
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;


    @Override
    public Page readPage(PageId pageId) {
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            randomAccessFile.seek((long)pageId.getPageNumber()*pageSize);
            if(randomAccessFile.read(buf)==-1){
                return null;
            }
            heapPage= new HeapPage((HeapPageId) pageId, buf);
            randomAccessFile.close();
        } catch (IOException e ) {
            e.printStackTrace();
        }
        return heapPage;
    }

    @Override
    public void writePage(Page page) throws IOException {
        HeapPageId heapPageId = (HeapPageId) page.getId();
        int size = BufferPool.getPageSize();
        int pageNumber = heapPageId.getPageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pageNumber* size);
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        if (!file.canRead() || !file.canWrite()) {
            throw new IOException();
        }
        List<Page> res = new ArrayList<>();
        //遍历全部page，找到空的tuple插入
        for(int i=0;i<numPages();i++){
            HeapPageId heapPageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
            if (heapPage == null || heapPage.getNumEmptySlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, heapPageId);
                continue;
            }
            heapPage.insertTuple(t);
            res.add(heapPage);
            return res;
        }
        //新建一个page
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        res.add(heapPage);
        return res;
    }

    /**
     * 计算page数
     * @return
     */
    public int numPages() {
        return (int)file.length()/BufferPool.getPageSize();
    }


    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> res = new ArrayList<>();
        HeapPageId heapPageId  = (HeapPageId) t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        if(heapPage==null){
            throw new DbException("null");
        }
        heapPage.deleteTuple(t);
        res.add(heapPage);
        return res;
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid,Permissions.READ_ONLY);
    }

    public class HeapFileIterator implements DbFileIterator{
        TransactionId tid;
        Permissions permissions;
        BufferPool bufferPool =Database.getBufferPool();
        Iterator<Tuple> iterator;  //每一页的迭代器
        int num;

        public HeapFileIterator(TransactionId tid,Permissions permissions){
            this.tid = tid;
            this.permissions = permissions;
        }

        /**
         * 获取第一页迭代器
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);
            HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);
            if(page==null){
                throw  new DbException("page null");
            }else{
                iterator = page.iterator();
            }
        }

        /**
         * 获取下一页迭代器
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public boolean nextPage() throws DbException, TransactionAbortedException {
            while(true){
                num++;
                if(num>=numPages()){
                    return false;
                }
                HeapPageId heapPageId = new HeapPageId(getId(), num);
                HeapPage page = (HeapPage)bufferPool.getPage(tid,heapPageId,permissions);
                if(page==null){
                    continue;
                }
                iterator = page.iterator();
                if(iterator.hasNext()){
                    return true;
                }
            }
        }


        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(iterator==null){
                return false;
            }
            if(iterator.hasNext()){
                return true;
            }else{
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(iterator==null){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }
}
