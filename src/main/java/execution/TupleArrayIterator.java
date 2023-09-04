package execution;

import storage.Tuple;
import storage.TupleDesc;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TupleArrayIterator implements OpIterator {

    private static final long serialVersionUID = 1L;
    final List<Tuple> tuples;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(List<Tuple> tuples) {
        this.tuples = tuples;
    }

    public void open() {
        it = tuples.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException {
        return it.next();
    }

    public void rewind() {
        it = tuples.iterator();
    }

    public TupleDesc getTupleDesc() {
        return tuples.get(0).getTupleDesc();
    }

    public void close() {
    }

}