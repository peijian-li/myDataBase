package transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionId implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final AtomicLong counter = new AtomicLong(0);
    private final long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof TransactionId)
            return false;
        TransactionId other = (TransactionId) obj;
        return myid == other.myid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (myid ^ (myid >>> 32));
        return result;
    }
}
