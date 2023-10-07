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
     * @param page
     * @throws IOException
     */
    public synchronized void logWrite(TransactionId tid, Page page) throws IOException  {
        log.info("WRITE, offset = " + randomAccessFile.getFilePointer());
        preAppend();
        randomAccessFile.writeInt(UPDATE_RECORD);
        randomAccessFile.writeLong(tid.getId());
        writePageData(randomAccessFile,page);
        randomAccessFile.writeLong(currentOffset);
        currentOffset = randomAccessFile.getFilePointer();
        log.info("WRITE OFFSET = " + currentOffset);
    }

    /**
     * 添加日志前预操作
     * @throws IOException
     */
    private void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            //清空日志文件
            recoveryUndecided = false;
            randomAccessFile.seek(0);
            randomAccessFile.setLength(0);
            currentOffset = randomAccessFile.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
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
     * 恢复数据库
     * @throws IOException
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                Map<Long, Map<PageId,Page>> redoMap = new HashMap<>();//key:事务id  value:事务对应页面
                randomAccessFile.seek(0);
                print();
                recoverSearch(randomAccessFile, redoMap);
                //写入页面，恢复数据库
                for(Long tid:redoMap.keySet()){
                    for(PageId pid:redoMap.get(tid).keySet()){
                        Page page = redoMap.get(tid).get(pid);
                        DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                        databaseFile.writePage(page);
                    }
                }
                redoMap.clear();
            }
        }
    }

    /**
     * 将需要恢复的事务和关联页面放入redoMap中
     * @param redoMap
     * @throws IOException
     */
    private void recoverSearch(RandomAccessFile randomAccessFile,Map<Long, Map<PageId,Page>> redoMap) throws IOException {
        Set<Long> commitTransaction=new HashSet<>();
        while(true){
            try{
                int type = randomAccessFile.readInt();
                long curTid = randomAccessFile.readLong();
                if(type==UPDATE_RECORD){
                    //update，将更新记录添加到redoMap
                    if(!redoMap.containsKey(curTid)){
                        redoMap.put(curTid,new HashMap<>());
                    }
                    Page page = readPageData(randomAccessFile);
                    redoMap.get(curTid).put(page.getId(),page);
                }else if(type==COMMIT_RECORD && redoMap.containsKey(curTid)){
                    //commit，需要恢复的事务
                    commitTransaction.add(curTid);
                }
                //跳过currentOffset
                randomAccessFile.seek(randomAccessFile.getFilePointer()+8);
            }catch (EOFException e){
                break;
            }
        }
        redoMap.entrySet().removeIf(longMapEntry -> !commitTransaction.contains(longMapEntry.getKey()));
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
        //刷盘
        randomAccessFile.getChannel().force(true);
    }
}
