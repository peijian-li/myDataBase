package storage;

public interface PageId {

    int[] serialize();

    int getTableId();

    int getPageNumber();

    int hashCode();

    boolean equals(Object o);


}
