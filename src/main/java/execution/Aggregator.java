package execution;

import storage.Tuple;

import java.io.Serializable;

public interface Aggregator extends Serializable {

    int NO_GROUPING = -1;

    enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT, SUM_COUNT, SC_AVG;

        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString()
        {
            if (this==MIN)
                return "min";
            if (this==MAX)
                return "max";
            if (this==SUM)
                return "sum";
            if (this==SUM_COUNT)
                return "sum_count";
            if (this==AVG)
                return "avg";
            if (this==COUNT)
                return "count";
            if (this==SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    void mergeTupleIntoGroup(Tuple tuple);

    OpIterator iterator();
}
