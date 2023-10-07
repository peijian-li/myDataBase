package storage;

import common.Database;
import common.DbException;
import common.Permissions;
import common.TransactionAbortedException;
import transaction.LockManager;
import transaction.TransactionId;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4096;
    public static final int DEFAULT_PAGES = 50;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private int numPages;
    private LRUCache<PageId,Page> buffer;
    private LockManager lockManager;

    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.buffer  = new LRUCache<>(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {

        //自旋获取锁，超时抛出异常
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000);
        while(!lockAcquired){
            long now = System.currentTimeMillis();
            if(now - start> timeout){
                throw new TransactionAbortedException();
            }
            lockAcquired = lockManager.acquireLock(tid,pid,perm);
        }

        //从缓存中读取page，没有则从磁盘中读取
        if (this.buffer.get(pid)==null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            //超出容量
            if (buffer.getSize() >= numPages) {
                evictPage();
            }
            buffer.put(pid, page);
            return page;
        }
        return this.buffer.get(pid);
    }

    /**
     * 淘汰页面，移除最久未使用的非脏页
     * @throws DbException
     */
    private synchronized void evictPage() throws DbException {
        LRUCache<PageId, Page>.Node head = buffer.getHead();
        LRUCache<PageId, Page>.Node tail = buffer.getTail();
        tail = tail.pre;
        while (head != tail) {
            Page page = tail.value;
            if (page != null && page.isDirty() == null) {
                buffer.remove(tail);
                return;
            }
            tail = tail.pre;
        }
        //没有非脏页，抛出异常
        throw new DbException("no dirty page");
    }

    /**
     * 淘汰指定页面
     * @param pid
     */
    public synchronized void discardPage(PageId pid) {
        LRUCache<PageId, Page>.Node head = buffer.getHead();
        LRUCache<PageId, Page>.Node tail = buffer.getTail();
        while(head!=tail){
            PageId key = head.key;
            if(key!=null && key.equals(pid)){
                buffer.remove(head);
                return;
            }
            head = head.next;
        }
    }


    /**
     * 释放页面和事务关联的锁
     * @param tid
     * @param pid
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid,pid);
    }


    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for(Page page : pages){
            Database.getLogFile().logWrite(tid,page);
            Database.getLogFile().force();
            page.markDirty(true,tid);
            buffer.put(page.getId(),page);
        }
    }


    public  void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid,t);
        for(Page page: pages){
            Database.getLogFile().logWrite(tid,page);
            Database.getLogFile().force();
            page.markDirty(true,tid);
        }
    }

    /**
     * 提交或回滚事务
     * @param tid
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid,true);
    }

    /**
     * 提交或回滚事务
     * @param tid
     * @param commit
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            rollback(tid);
        }
        lockManager.releaseAllLock(tid);
    }

    /**
     * 将事务更新的脏页刷盘
     * @param tid
     * @throws IOException
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        LRUCache<PageId, Page>.Node head = buffer.getHead();
        LRUCache<PageId, Page>.Node tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            if(page!=null && page.isDirty()!=null&&page.isDirty().equals(tid) ){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try{
                    page.markDirty(false,null);
                    dbFile.writePage(page);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }

    /**
     * 回滚
     * @param tid
     */
    private void rollback(TransactionId tid) {
        LRUCache<PageId, Page>.Node head = buffer.getHead();
        LRUCache<PageId, Page>.Node tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            LRUCache<PageId, Page>.Node next = head.next;
            if(page!=null && page.isDirty()!=null && page.isDirty().equals(tid)){
                buffer.remove(head);
                //从磁盘中重新读取页面
                Page page1 = null;
                try {
                    page1 = Database.getBufferPool().getPage(tid, page.getId(), Permissions.READ_ONLY);
                    page1.markDirty(false,null);
                } catch (TransactionAbortedException | DbException e) {
                    e.printStackTrace();
                }

            }
            head = next;
        }
    }

    /**
     * 全部脏页刷盘
     * @throws IOException
     */
    public synchronized void flushAllPages() throws IOException {
        LRUCache<PageId, Page>.Node head = buffer.getHead();
        LRUCache<PageId, Page>.Node tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            if(page!=null && page.isDirty()!=null){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try{
                    page.markDirty(false,null);
                    dbFile.writePage(page);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }


}
