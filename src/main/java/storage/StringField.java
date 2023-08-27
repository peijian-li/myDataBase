package storage;

import common.Type;
import execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

public class StringField implements Field{

    private final String value;
    private final int maxSize;

    public String getValue() {
        return value;
    }

    public StringField(String s, int maxSize) {
        this.maxSize = maxSize;

        if (s.length() > maxSize)
            value = s.substring(0, maxSize);
        else
            value = s;
    }


    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        String s = value;
        int overflow = maxSize - s.length();
        if (overflow < 0) {
            s = s.substring(0, maxSize);
        }
        dos.writeInt(s.length());
        dos.writeBytes(s);
        while (overflow-- > 0)
            dos.write((byte) 0);
    }

    @Override
    public boolean compare(Predicate.Op op, Field val) {
        StringField iVal = (StringField) val;
        int cmpVal = value.compareTo(iVal.value);

        switch (op) {
            case EQUALS:
                return cmpVal == 0;

            case NOT_EQUALS:
                return cmpVal != 0;

            case GREATER_THAN:
                return cmpVal > 0;

            case GREATER_THAN_OR_EQ:
                return cmpVal >= 0;

            case LESS_THAN:
                return cmpVal < 0;

            case LESS_THAN_OR_EQ:
                return cmpVal <= 0;

            case LIKE:
                return value.contains(iVal.value);
        }

        return false;
    }

    @Override
    public Type getType() {
        return Type.STRING_TYPE;
    }

    public String toString() {
        return value;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public boolean equals(Object field) {
        if (!(field instanceof StringField)) return false;
        return ((StringField) field).value.equals(value);
    }
}
