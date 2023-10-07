package storage;

import transaction.TransactionId;

public interface Page {

    PageId getId();

    /**
     * 非脏页返回null，脏页返回修改的事务id
     * @return
     */
    TransactionId isDirty();

    void markDirty(boolean dirty, TransactionId tid);

    /**
     * page转换为byte数组
     * @return
     */
    byte[] getPageData();


}
