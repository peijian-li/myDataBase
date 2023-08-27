package storage;

import common.Type;
import execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntField implements Field{

    private final int value;

    public IntField(int i) {
        value = i;
    }

    public int getValue() {
        return value;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    @Override
    public boolean compare(Predicate.Op op, Field val) {
        IntField iVal = (IntField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return value == iVal.value;
            case NOT_EQUALS:
                return value != iVal.value;
            case GREATER_THAN:
                return value > iVal.value;
            case GREATER_THAN_OR_EQ:
                return value >= iVal.value;
            case LESS_THAN:
                return value < iVal.value;
            case LESS_THAN_OR_EQ:
                return value <= iVal.value;
        }

        return false;
    }

    @Override
    public Type getType() {
        return Type.INT_TYPE;
    }

    public String toString() {
        return Integer.toString(value);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        if (!(field instanceof IntField)) return false;
        return ((IntField) field).value == value;
    }
}
