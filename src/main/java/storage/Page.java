package storage;

import transaction.TransactionId;

public interface Page {

    PageId getId();

    TransactionId isDirty();

    void markDirty(boolean dirty, TransactionId tid);

    /**
     * page转换为byte数组
     * @return
     */
    byte[] getPageData();


    Page getBeforeImage();


    void setBeforeImage();
}
