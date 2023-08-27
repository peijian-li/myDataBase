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

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided;

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;
    int totalRecords = 0;

    final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();

    public LogFile(File file) throws IOException {
        this.logFile = file;
        raf = new RandomAccessFile(file, "rw");
        recoveryUndecided = true;
    }

    public synchronized  void logTractionBegin(TransactionId tid) throws IOException {
        log.info("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.print("logTractionBegin: already began this tid\n");
            throw new IOException("double logTractionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();
        log.info("BEGIN OFFSET = " + currentOffset);
    }

    public void logAbort(TransactionId tid) throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                rollback(tid);
                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    private void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    private void rollback(TransactionId tid) throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                // some code goes here
                preAppend();
                long tidId = tid.getId();
                Long begin = tidToFirstLogRecord.get(tidId);
                raf.seek(begin);
                while(true){
                    try{
                        int type = raf.readInt();
                        Long curTid = raf.readLong();
                        if(curTid!=tidId){
                            //如果不是当前的tid，就直接跳过
                            if(type==3){
                                //update record 还要跳过页数据
                                readPageData(raf);
                                readPageData(raf);
                            }
                        }else{
                            if(type==3){
                                //只需要恢复到最初的状态就行
                                Page before = readPageData(raf);
                                Page after = readPageData(raf);
                                DbFile databaseFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
                                databaseFile.writePage(before);
                                Database.getBufferPool().discardPage(after.getId());
                                raf.seek(raf.getFilePointer()+8);
                                break;
                            }
                        }
                        raf.seek(raf.getFilePointer()+8);
                    }catch (EOFException e){
                        break;
                    }
                }
            }
        }
    }

    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        log.info("COMMIT " + tid.getId());
        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }


    public  synchronized void logWrite(TransactionId tid, Page before, Page after) throws IOException  {
        log.info("WRITE, offset = " + raf.getFilePointer());
        preAppend();

        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());
        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        log.info("WRITE OFFSET = " + currentOffset);
    }

    private void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();
        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
    }

    private Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData);

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

    public synchronized void shutdown() {
        try {
            logCheckpoint();
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    private void logCheckpoint() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1);

                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    log.info("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
            }
        }
        logTruncate();
    }

    private synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);


        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                log.info("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                    case UPDATE_RECORD:
                        Page before = readPageData(raf);
                        Page after = readPageData(raf);

                        writePageData(logNew, before);
                        writePageData(logNew, after);
                        break;
                    case CHECKPOINT_RECORD:
                        int numXactions = raf.readInt();
                        logNew.writeInt(numXactions);
                        while (numXactions-- > 0) {
                            long xid = raf.readLong();
                            long xoffset = raf.readLong();
                            logNew.writeLong(xid);
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                        }
                        break;
                    case BEGIN_RECORD:
                        tidToFirstLogRecord.put(record_tid,newStart);
                        break;
                }

                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        log.info("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();;
    }

    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                HashMap<Long, List<Page[]>> undoMap = new HashMap<>();
                raf.seek(0);
                print();
                long checkpoint = raf.readLong();
                if(checkpoint!=-1){
                    HashMap<Long, Long> tidPos = new HashMap<>();
                    raf.seek(checkpoint);
                    //跳过record type和tid
                    raf.seek(raf.getFilePointer()+12);
                    //获取正在进行事务的个数
                    int num = raf.readInt();
                    while(num>0){
                        //获取每一个事务的tid和第一条log record OFFSET
                        long curTid = raf.readLong();
                        long offset = raf.readLong();
                        tidPos.put(curTid,offset);
                        num--;
                    }
                    for(Long pos:tidPos.keySet()){
                        raf.seek(tidPos.get(pos));
                        recoverSearch(raf,undoMap);
                    }
                }else{
                    System.out.println(raf.getFilePointer() + "-----------");
                    recoverSearch(raf, undoMap);
                }
                //进行undo操作
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

    private void recoverSearch(RandomAccessFile raf,Map<Long,List<Page[]>> map) throws IOException {
        while(true){
            try{
                int type = raf.readInt();
                long curTid = raf.readLong();
                if(type==3){
                    if(!map.containsKey(curTid)){
                        map.put(curTid,new ArrayList<>());
                    }
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);
                    map.get(curTid).add(new Page[]{before,after});
                }else if(type==2 && map.containsKey(curTid)){
                    Page[] pages = map.get(curTid).get(map.get(curTid).size() - 1);
                    Page after = pages[1];
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(after.getId().getTableId());
                    databaseFile.writePage(after);
                    map.remove(curTid);
                }else if(type==1 && map.containsKey(curTid)){
                    Page[] pages = map.get(curTid).get(0);
                    Page before = pages[0];
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
                    databaseFile.writePage(before);
                    map.remove(curTid);
                }
                raf.seek(raf.getFilePointer()+8);
            }catch (EOFException e){
                break;
            }
        }
    }

    private void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                    case BEGIN_RECORD:
                        System.out.println(" (BEGIN)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case ABORT_RECORD:
                        System.out.println(" (ABORT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case COMMIT_RECORD:
                        System.out.println(" (COMMIT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;

                    case CHECKPOINT_RECORD:
                        System.out.println(" (CHECKPOINT)");
                        int numTransactions = raf.readInt();
                        System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                        while (numTransactions-- > 0) {
                            long tid = raf.readLong();
                            long firstRecord = raf.readLong();
                            System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                            System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                        }
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;
                    case UPDATE_RECORD:
                        System.out.println(" (UPDATE)");

                        long start = raf.getFilePointer();
                        Page before = readPageData(raf);

                        long middle = raf.getFilePointer();
                        Page after = readPageData(raf);

                        System.out.println(start + ": before image table id " + before.getId().getTableId());
                        System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                        System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                        System.out.println(middle + ": after image table id " + after.getId().getTableId());
                        System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                        System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;
                }

            } catch (EOFException e) {
                e.printStackTrace();
                break;
            }
        }

        raf.seek(curOffset);
    }



    public  synchronized void force() throws IOException {
        //进行刷盘
        raf.getChannel().force(true);
    }
}
