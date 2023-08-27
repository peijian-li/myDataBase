package common;

import storage.BufferPool;
import storage.LogFile;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class Database {

    private static final AtomicReference<Database> INSTANCE = new AtomicReference<>(new Database());
    private static final String LOG_FILENAME = "log";

    private final Catalog catalog;
    private final BufferPool bufferpool;
    private final LogFile logfile;

    private Database() {
        catalog = new Catalog();
        bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOG_FILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        logfile = tmp;
    }


    public static LogFile getLogFile() {
        return INSTANCE.get().logfile;
    }

    public static BufferPool getBufferPool() {
        return INSTANCE.get().bufferpool;
    }

    public static Catalog getCatalog() {
        return INSTANCE.get().catalog;
    }

}
