package optimizer;

import execution.Predicate;

public class StringHistogram {

    final IntHistogram hist;

    public StringHistogram(int buckets) {
        hist = new IntHistogram(buckets, minVal(), maxVal());
    }


    private int stringToInt(String s) {
        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }

        if (!(s.equals("") || s.equals("zzzz"))) {
            if (v < minVal()) {
                v = minVal();
            }

            if (v > maxVal()) {
                v = maxVal();
            }
        }

        return v;
    }

    int maxVal() {
        return stringToInt("zzzz");
    }

    int minVal() {
        return stringToInt("");
    }

    public void addValue(String s) {
        int val = stringToInt(s);
        hist.addValue(val);
    }

    public double estimateSelectivity(Predicate.Op op, String s) {
        int val = stringToInt(s);
        return hist.estimateSelectivity(op, val);
    }
}
