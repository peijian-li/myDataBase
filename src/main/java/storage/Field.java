package storage;

import common.Type;
import execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Field {

    void serialize(DataOutputStream dos) throws IOException;

    boolean compare(Predicate.Op op, Field value);

    Type getType();

    int hashCode();

    boolean equals(Object field);

    String toString();
}
