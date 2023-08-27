package execution;

import lombok.AllArgsConstructor;
import lombok.Getter;
import storage.Tuple;

import java.io.Serializable;

@AllArgsConstructor
@Getter
public class JoinPredicate implements Serializable {
    private static final long serialVersionUID = 1L;

    private int field1;
    private Predicate.Op op;
    private int field2;

    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1).compare(this.op,t2.getField(field2));
    }

}
