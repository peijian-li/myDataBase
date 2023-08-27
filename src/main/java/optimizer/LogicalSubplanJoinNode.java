package optimizer;

import execution.OpIterator;
import execution.Predicate;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class LogicalSubplanJoinNode extends LogicalJoinNode{
    public OpIterator subPlan;

    public LogicalSubplanJoinNode(String table1, String joinField1, OpIterator sp, Predicate.Op pred) {
        t1Alias = table1;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length>1)
            f1PureName = tmps[tmps.length-1];
        else
            f1PureName=joinField1;
        f1QuantifiedName=t1Alias+"."+f1PureName;
        subPlan = sp;
        p = pred;
    }

    public LogicalSubplanJoinNode swapInnerOuter() {
        return new LogicalSubplanJoinNode(t1Alias,f1PureName,subPlan, p);
    }

    @Override
    public int hashCode() {
        return t1Alias.hashCode() + f1PureName.hashCode() + subPlan.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        if (!(o instanceof LogicalSubplanJoinNode))
            return false;

        return (j2.t1Alias.equals(t1Alias)  && j2.f1PureName.equals(f1PureName) && ((LogicalSubplanJoinNode)o).subPlan.equals(subPlan));
    }
}
