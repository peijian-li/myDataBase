package execution;

import storage.Field;

import java.io.Serializable;

public class IndexPredicate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Predicate.Op op;
    private final Field fieldvalue;

    public IndexPredicate(Predicate.Op op, Field fvalue) {
        this.op = op;
        this.fieldvalue = fvalue;
    }

    public Field getField() {
        return fieldvalue;
    }

    public Predicate.Op getOp() {
        return op;
    }

    public boolean equals(IndexPredicate ipd) {
        if (ipd == null)
            return false;
        return (op.equals(ipd.op) && fieldvalue.equals(ipd.fieldvalue));
    }
}
