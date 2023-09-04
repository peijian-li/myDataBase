package storage;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import common.Database;
import lombok.extern.slf4j.Slf4j;
import transaction.TransactionId;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

@Slf4j
public class LogFile {

    private final File logFile;
    private RandomAccessFile randomAccessFile;
    private Boolean recoveryUndecided;

    private static final int ABORT_RECORD = 1;
    private static final int COMMIT_RECORD = 2;
    private static final int UPDATE_RECORD = 3;
    private static final int BEGIN_RECORD = 4;
    private static final int CHECKPOINT_RECORD = 5;
    private static final long NO_CHECKPOINT_ID = -1;

    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    private long currentOffset = -1;
    private int totalRecords = 0;

    private final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();//key:事务id value:事务开始日志位置

    public LogFile(File file) throws IOException {
        this.logFile = file;
        randomAccessFile = new RandomAccessFile(file, "rw");
        recoveryUndecided = true;//true:不需要恢复数据库 false:需要
    }

    /**
     * 记录事务开启
     * @param tid
     * @throws IOException
     */
    public synchronized void logTractionBegin(TransactionId tid) throws IOException {
        log.info("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            log.error("logTractionBegin: already began this tid\n");
            throw new IOException("double logTractionBegin()");
        }
        preAppend();
        randomAccessFile.writeInt(BEGIN_RECORD);
        randomAccessFile.writeLong(tid.getId());
        randomAccessFile.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = randomAccessFile.getFilePointer();
        log.info("BEGIN OFFSET = " + currentOffset);
    }

    /**
     * 记录事务回滚
     * @param tid
     * @throws IOException
     */
    public void logAbort(TransactionId tid) throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                rollback(tid);
                randomAccessFile.writeInt(ABORT_RECORD);
                randomAccessFile.writeLong(tid.getId());
                randomAccessFile.writeLong(currentOffset);
                currentOffset = randomAccessFile.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /**
     * 添加日志前预操作
     * @throws IOException
     */
    private void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            //不需要恢复数据库时清空日志文件
            recoveryUndecided = false;
            randomAccessFile.seek(0);
            randomAccessFile.setLength(0);
            randomAccessFile.writeLong(NO_CHECKPOINT_ID);
            randomAccessFile.seek(randomAccessFile.length());
            currentOffset = randomAccessFile.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }

    /**
     * 回滚事务
     * @param tid
     * @throws NoSuchElementException
     * @throws IOException
     */
    private void rollback(TransactionId tid) throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                long tidId = tid.getId();
                Long begin = tidToFirstLogRecord.get(tidId);
                randomAccessFile.seek(begin);
                while(true){
                    try{
                        int type = randomAccessFile.readInt();
                        Long curTid = randomAccessFile.readLong();
                        if(curTid!=tidId){
                            //如果不是当前的tid，就直接跳过
                            if(type==3){
                                //如果是update record 还要跳过页数据
                                readPageData(randomAccessFile);
                                readPageData(randomAccessFile);
                            }
                        }else{
                            //不是update record，直接跳过
                            if(type==3){
                                //写入旧页
                                Page before = readPageData(randomAccessFile);
                                Page after = readPageData(randomAccessFile);
                                DbFile databaseFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
                                databaseFile.writePage(before);
                                Database.getBufferPool().discardPage(after.getId());
                                randomAccessFile.seek(randomAccessFile.getFilePointer()+8);
                                break;
                            }
                        }
                        randomAccessFile.seek(randomAccessFile.getFilePointer()+8);
                    }catch (EOFException e){
                        break;
                    }
                }
            }
        }
    }

    /**
     * 记录事务提交
     * @param tid
     * @throws IOException
     */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        log.info("COMMIT " + tid.getId());
        randomAccessFile.writeInt(COMMIT_RECORD);
        randomAccessFile.writeLong(tid.getId());
        randomAccessFile.writeLong(currentOffset);
        currentOffset = randomAccessFile.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /**
     * 记录事务更新页面
     * @param tid
     * @param before
     * @param after
     * @throws IOException
     */
    public synchronized void logWrite(TransactionId tid, Page before, Page after) throws IOException  {
        log.info("WRITE, offset = " + randomAccessFile.getFilePointer());
        preAppend();
        randomAccessFile.writeInt(UPDATE_RECORD);
        randomAccessFile.writeLong(tid.getId());
        writePageData(randomAccessFile,before);
        writePageData(randomAccessFile,after);
        randomAccessFile.writeLong(currentOffset);
        currentOffset = randomAccessFile.getFilePointer();
        log.info("WRITE OFFSET = " + currentOffset);
    }


    private void writePageData(RandomAccessFile randomAccessFile, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();
        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        randomAccessFile.writeUTF(pageClassName);
        randomAccessFile.writeUTF(idClassName);

        randomAccessFile.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            randomAccessFile.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        randomAccessFile.writeInt(pageData.length);
        randomAccessFile.write(pageData);
    }

    private Page readPageData(RandomAccessFile randomAccessFile) throws IOException {
        PageId pid;
        Page newPage;

        String pageClassName = randomAccessFile.readUTF();
        String idClassName = randomAccessFile.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = randomAccessFile.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = randomAccessFile.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = randomAccessFile.readInt();

            byte[] pageData = new byte[pageSize];
            randomAccessFile.read(pageData);

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e){
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /**
     *  关闭数据库
     */
    public synchronized void shutdown() {
        try {
            logCheckpoint();
            randomAccessFile.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * 记录检查点
     * @throws IOException
     */
    private void logCheckpoint() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = randomAccessFile.getFilePointer();
                randomAccessFile.writeInt(CHECKPOINT_RECORD);
                randomAccessFile.writeLong(-1);
                randomAccessFile.writeInt(keys.size());
                //记录每个事务开始日志位置
                while (els.hasNext()) {
                    Long key = els.next();
                    log.info("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    randomAccessFile.writeLong(key);
                    randomAccessFile.writeLong(tidToFirstLogRecord.get(key));
                }
                endCpOffset = randomAccessFile.getFilePointer();
                randomAccessFile.seek(0);
                randomAccessFile.writeLong(startCpOffset);
                randomAccessFile.seek(endCpOffset);
                randomAccessFile.writeLong(currentOffset);
                currentOffset = randomAccessFile.getFilePointer();
            }
        }
        logTruncate();
    }

    /**
     *  截断日志，将旧日志复制到新日志中
     * @throws IOException
     */
    private synchronized void logTruncate() throws IOException {
        preAppend();
        randomAccessFile.seek(0);
        long cpLoc = randomAccessFile.readLong();
        long minLogRecord = cpLoc;
        //读取每个事务开始日志位置，计算最小日志位置
        if (cpLoc != -1L) {
            randomAccessFile.seek(cpLoc);
            int cpType = randomAccessFile.readInt();
            long cpTid = randomAccessFile.readLong();
            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }
            int numOutstanding = randomAccessFile.readInt();
            for (int i = 0; i < numOutstanding; i++) {
                long tid = randomAccessFile.readLong();
                long firstLogRecord = randomAccessFile.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }
        File newFile = new File("logTmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);
        randomAccessFile.seek(minLogRecord);
        //从最小日志位置开始，将log复制到logTmp
        while (true) {
            try {
                int type = randomAccessFile.readInt();
                long record_tid = randomAccessFile.readLong();
                long newStart = logNew.getFilePointer();
                log.info("NEW START = " + newStart);
                logNew.writeInt(type);
                logNew.writeLong(record_tid);
                switch (type) {
                    case UPDATE_RECORD:
                        Page before = readPageData(randomAccessFile);
                        Page after = readPageData(randomAccessFile);
                        writePageData(logNew, before);
                        writePageData(logNew, after);
                        break;
                    case CHECKPOINT_RECORD:
                        int numXactions = randomAccessFile.readInt();
                        logNew.writeInt(numXactions);
                        while (numXactions-- > 0) {
                            long xid = randomAccessFile.readLong();
                            long xoffset = randomAccessFile.readLong();
                            logNew.writeLong(xid);
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                        }
                        break;
                    case BEGIN_RECORD:
                        tidToFirstLogRecord.put(record_tid,newStart);
                        break;
                }
                logNew.writeLong(newStart);
                randomAccessFile.readLong();
            } catch (EOFException e) {
                break;
            }
        }
        log.info("TRUNCATING LOG;  WAS " + randomAccessFile.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (randomAccessFile.length() - minLogRecord));
        randomAccessFile.close();
        logFile.delete();
        newFile.renameTo(logFile);
        randomAccessFile = new RandomAccessFile(logFile, "rw");
        randomAccessFile.seek(randomAccessFile.length());
        newFile.delete();
        currentOffset = randomAccessFile.getFilePointer();;
    }

    /**
     * 恢复数据库
     * @throws IOException
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                HashMap<Long, List<Page[]>> undoMap = new HashMap<>();//key:事务id  value:事务对应页面
                randomAccessFile.seek(0);
                print();
                long checkpoint = randomAccessFile.readLong();
                if(checkpoint!=-1){
                    HashMap<Long, Long> tidPos = new HashMap<>();
                    randomAccessFile.seek(checkpoint);
                    //跳过recordType和tid
                    randomAccessFile.seek(randomAccessFile.getFilePointer()+12);
                    int num = randomAccessFile.readInt();
                    while(num>0){
                        long curTid = randomAccessFile.readLong();
                        long offset = randomAccessFile.readLong();
                        tidPos.put(curTid,offset);
                        num--;
                    }
                    for(Long pos:tidPos.keySet()){
                        randomAccessFile.seek(tidPos.get(pos));
                        recoverSearch(randomAccessFile,undoMap);
                    }
                }else{
                    System.out.println(randomAccessFile.getFilePointer() + "-----------");
                    recoverSearch(randomAccessFile, undoMap);
                }
                //写入旧页面，恢复数据库
                for(Long tid:undoMap.keySet()){
                    Page[] pages = undoMap.get(tid).get(0);
                    Page before = pages[0];
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
                    databaseFile.writePage(before);
                }
                undoMap.clear();
            }
        }
    }

    /**
     * 将需要恢复的事务和页面放入undoMap中
     * @param randomAccessFile
     * @param map
     * @throws IOException
     */
    private void recoverSearch(RandomAccessFile randomAccessFile,Map<Long,List<Page[]>> map) throws IOException {
        while(true){
            try{
                int type = randomAccessFile.readInt();
                long curTid = randomAccessFile.readLong();
                if(type==UPDATE_RECORD){
                    if(!map.containsKey(curTid)){
                        map.put(curTid,new ArrayList<>());
                    }
                    Page before = readPageData(randomAccessFile);
                    Page after = readPageData(randomAccessFile);
                    map.get(curTid).add(new Page[]{before,after});
                }else if(type==COMMIT_RECORD && map.containsKey(curTid)){
                    //崩溃前提交，将最新页面刷盘，从undoMap中移除
                    Page[] pages = map.get(curTid).get(map.get(curTid).size() - 1);
                    Page after = pages[1];
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(after.getId().getTableId());
                    databaseFile.writePage(after);
                    map.remove(curTid);
                }else if(type==ABORT_RECORD && map.containsKey(curTid)){
                    //崩溃前回滚，将最旧页面刷盘，从undoMap中移除
                    Page[] pages = map.get(curTid).get(0);
                    Page before = pages[0];
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
                    databaseFile.writePage(before);
                    map.remove(curTid);
                }
                randomAccessFile.seek(randomAccessFile.getFilePointer()+8);
            }catch (EOFException e){
                break;
            }
        }
    }

    /**
     * 打印日志
     * @throws IOException
     */
    private void print() throws IOException {
        long curOffset = randomAccessFile.getFilePointer();

        randomAccessFile.seek(0);

        System.out.println("0: checkpoint record at offset " + randomAccessFile.readLong());

        while (true) {
            try {
                int cpType = randomAccessFile.readInt();
                long cpTid = randomAccessFile.readLong();

                System.out.println((randomAccessFile.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((randomAccessFile.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                    case BEGIN_RECORD:
                        System.out.println(" (BEGIN)");
                        System.out.println(randomAccessFile.getFilePointer() + ": RECORD START OFFSET: " + randomAccessFile.readLong());
                        break;
                    case ABORT_RECORD:
                        System.out.println(" (ABORT)");
                        System.out.println(randomAccessFile.getFilePointer() + ": RECORD START OFFSET: " + randomAccessFile.readLong());
                        break;
                    case COMMIT_RECORD:
                        System.out.println(" (COMMIT)");
                        System.out.println(randomAccessFile.getFilePointer() + ": RECORD START OFFSET: " + randomAccessFile.readLong());
                        break;

                    case CHECKPOINT_RECORD:
                        System.out.println(" (CHECKPOINT)");
                        int numTransactions = randomAccessFile.readInt();
                        System.out.println((randomAccessFile.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                        while (numTransactions-- > 0) {
                            long tid = randomAccessFile.readLong();
                            long firstRecord = randomAccessFile.readLong();
                            System.out.println((randomAccessFile.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                            System.out.println((randomAccessFile.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                        }
                        System.out.println(randomAccessFile.getFilePointer() + ": RECORD START OFFSET: " + randomAccessFile.readLong());

                        break;
                    case UPDATE_RECORD:
                        System.out.println(" (UPDATE)");

                        long start = randomAccessFile.getFilePointer();
                        Page before = readPageData(randomAccessFile);

                        long middle = randomAccessFile.getFilePointer();
                        Page after = readPageData(randomAccessFile);

                        System.out.println(start + ": before image table id " + before.getId().getTableId());
                        System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                        System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                        System.out.println(middle + ": after image table id " + after.getId().getTableId());
                        System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                        System.out.println((middle + INT_SIZE) + " TO " + (randomAccessFile.getFilePointer()) + ": page data");

                        System.out.println(randomAccessFile.getFilePointer() + ": RECORD START OFFSET: " + randomAccessFile.readLong());

                        break;
                }

            } catch (EOFException e) {
                e.printStackTrace();
                break;
            }
        }

        randomAccessFile.seek(curOffset);
    }



    public synchronized void force() throws IOException {
        //进行刷盘
        randomAccessFile.getChannel().force(true);
    }
}
