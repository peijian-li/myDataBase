package execution;

import lombok.AllArgsConstructor;
import lombok.Data;
import storage.Field;
import storage.Tuple;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class Predicate implements Serializable{

    private static final long serialVersionUID = 1L;

    private int field;
    private Op op;
    private Field operand;

    public enum Op implements Serializable {

        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        private static final long serialVersionUID = 1L;

        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }


    }

    public boolean filter(Tuple t) {
        Field field = t.getField(this.field);
        return field.compare(this.op,this.operand);
    }


}
