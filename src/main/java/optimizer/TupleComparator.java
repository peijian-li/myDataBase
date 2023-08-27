package optimizer;

import execution.Predicate;
import storage.Field;
import storage.Tuple;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {
    final int field;
    final boolean asc;

    public TupleComparator(int field, boolean asc) {
        this.field = field;
        this.asc = asc;
    }

    public int compare(Tuple o1, Tuple o2) {
        Field t1 = (o1).getField(field);
        Field t2 = (o2).getField(field);
        if (t1.compare(Predicate.Op.EQUALS, t2))
            return 0;
        if (t1.compare(Predicate.Op.GREATER_THAN, t2))
            return asc ? 1 : -1;
        else
            return asc ? -1 : 1;
    }
}
