package optimizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanCache {

    private Map<Set<LogicalJoinNode>, List<LogicalJoinNode>> bestOrders= new HashMap<>();
    private Map<Set<LogicalJoinNode>,Double> bestCosts= new HashMap<>();
    private Map<Set<LogicalJoinNode>,Integer> bestCardinalities = new HashMap<>();

    public void addPlan(Set<LogicalJoinNode> s, double cost, int card, List<LogicalJoinNode> order) {
        bestOrders.put(s,order);
        bestCosts.put(s,cost);
        bestCardinalities.put(s,card);
    }

    public List<LogicalJoinNode> getOrder(Set<LogicalJoinNode> s) {
        return bestOrders.get(s);
    }

    public double getCost(Set<LogicalJoinNode> s) {
        return bestCosts.get(s);
    }

    public int getCard(Set<LogicalJoinNode> s) {
        return bestCardinalities.get(s);
    }
}
