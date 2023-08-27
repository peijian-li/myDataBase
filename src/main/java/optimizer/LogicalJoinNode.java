package optimizer;

import execution.Predicate;

public class LogicalJoinNode {

    public String t1Alias;

    public String t2Alias;

    public String f1PureName;

    public String f1QuantifiedName;

    public String f2PureName;

    public String f2QuantifiedName;

    public Predicate.Op p;

    public LogicalJoinNode() {
    }

    public LogicalJoinNode(String table1, String table2, String joinField1, String joinField2, Predicate.Op pred) {
        t1Alias = table1;
        t2Alias = table2;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length>1)
            f1PureName = tmps[tmps.length-1];
        else
            f1PureName=joinField1;
        tmps = joinField2.split("[.]");
        if (tmps.length>1)
            f2PureName = tmps[tmps.length-1];
        else
            f2PureName = joinField2;
        p = pred;
        this.f1QuantifiedName = t1Alias+"."+this.f1PureName;
        this.f2QuantifiedName = t2Alias+"."+this.f2PureName;
    }


    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newp;
        if (p == Predicate.Op.GREATER_THAN)
            newp = Predicate.Op.LESS_THAN;
        else if (p == Predicate.Op.GREATER_THAN_OR_EQ)
            newp = Predicate.Op.LESS_THAN_OR_EQ;
        else if (p == Predicate.Op.LESS_THAN)
            newp = Predicate.Op.GREATER_THAN;
        else if (p == Predicate.Op.LESS_THAN_OR_EQ)
            newp = Predicate.Op.GREATER_THAN_OR_EQ;
        else
            newp = p;
        return new LogicalJoinNode(t2Alias,t1Alias,f2PureName,f1PureName, newp);
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof LogicalJoinNode)) return false;
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        return (j2.t1Alias.equals(t1Alias)  || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias)  || j2.t2Alias.equals(t2Alias));
    }

    @Override public String toString() {
        return t1Alias + ":" + t2Alias ;//+ ";" + f1 + " " + p + " " + f2;
    }

    @Override public int hashCode() {
        return t1Alias.hashCode() + t2Alias.hashCode() + f1PureName.hashCode() + f2PureName.hashCode();
    }

}
