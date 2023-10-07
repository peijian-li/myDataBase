package transaction;

import common.Database;

import java.io.IOException;

public class Transaction {
    private final TransactionId tid;
    public volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }


    public void start() {
        started = true;
        try {
            Database.getLogFile().logTractionBegin(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TransactionId getId() {
        return tid;
    }


    public void commit() throws IOException {
        transactionComplete(false);
    }


    public void abort() throws IOException {
        transactionComplete(true);
    }


    public void transactionComplete(boolean abort) throws IOException {
        if (started) {
            if (abort) {
                Database.getLogFile().logAbort(tid);
            }else{
                Database.getLogFile().logCommit(tid);
            }
            Database.getBufferPool().transactionComplete(tid, !abort);
            started = false;
        }
    }
}
