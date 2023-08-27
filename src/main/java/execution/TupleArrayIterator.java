package execution;

import storage.Tuple;
import storage.TupleDesc;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TupleArrayIterator implements OpIterator {

    private static final long serialVersionUID = 1L;
    final List<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(List<Tuple> tups) {
        this.tups = tups;
    }

    public void open() {
        it = tups.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException {
        return it.next();
    }

    public void rewind() {
        it = tups.iterator();
    }

    public TupleDesc getTupleDesc() {
        return tups.get(0).getTupleDesc();
    }

    public void close() {
    }

}