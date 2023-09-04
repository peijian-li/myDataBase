package optimizer;


import common.Database;
import common.ParseException;
import execution.Join;
import execution.JoinPredicate;
import execution.OpIterator;
import execution.Predicate;
import lombok.AllArgsConstructor;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.util.*;

@AllArgsConstructor
public class JoinOptimizer {
    private LogicalPlan p;
    private List<LogicalJoinNode> joins;

    public static OpIterator instantiateJoin(LogicalJoinNode lj, OpIterator plan1, OpIterator plan2) throws ParseException {
        int t1id = 0, t2id = 0;
        OpIterator j;

        //1. 通过字段名获取字段
        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParseException("Unknown field " + lj.f1QuantifiedName);
        }
        if (lj instanceof LogicalSubPlanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParseException("Unknown field " + lj.f2QuantifiedName);
            }
        }

        //2. join
        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);
        j = new Join(p, plan1, plan2);
        return j;
    }

    /**
     * 估算两表join代价
     * @param j
     * @param cardinality1 表1基数
     * @param cardinality2 表2基数
     * @param cost1 表1扫描代价
     * @param cost2 表2扫描代价
     * @return
     */
    public double estimateJoinCost(LogicalJoinNode j, int cardinality1, int cardinality2, double cost1, double cost2) {
        if (j instanceof LogicalSubPlanJoinNode) {
            //子查询
            return cardinality1 + cost1 + cost2;
        } else {
            return cost1 + cardinality1 * cost2 + cardinality1 * cardinality2;
        }
    }

    /**
     * 估算基数
     * @param j
     * @param cardinality1
     * @param cardinality2
     * @param t1pkey
     * @param t2pkey
     * @param stats
     * @return
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int cardinality1, int cardinality2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubPlanJoinNode) {
            //子查询
            return cardinality1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias, j.f1PureName, j.f2PureName, cardinality1, cardinality2, t1pkey, t2pkey, stats, p.getTableAliasToIdMapping());
        }
    }

    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   String table1Alias, String table2Alias, String field1PureName, String field2PureName,
                                                   int cardinality1, int cardinality2,
                                                   boolean t1pkey, boolean t2pkey,
                                                   Map<String, TableStats> stats, Map<String, Integer> tableAliasToId) {
        int card = 1;
        //联表操作为=或!=
        if(joinOp.equals(Predicate.Op.EQUALS)){
            if(!t1pkey&&!t2pkey){
                return Math.max(cardinality1,cardinality2);
            }else if(!t2pkey){
                return cardinality2;
            }else if(!t1pkey){
                return cardinality1;
            }else{
                return Math.min(cardinality1,cardinality2);
            }
        }else if(joinOp.equals(Predicate.Op.NOT_EQUALS)){
            if(!t1pkey && !t2pkey){
                return cardinality1 * cardinality2 - Math.max(cardinality1,cardinality2);
            }else if (!t2pkey){
                return cardinality2*cardinality1 - cardinality2;
            }else if(!t1pkey){
                return cardinality1*cardinality2 - cardinality1;
            }else{
                return cardinality1*cardinality2 - Math.min(cardinality1,cardinality2);
            }
        }
        //如果不是=或!=，是很难估计基数的
        //输出的数量应该与输入的数量是成比例的，可以预估一个固定的分数代表range scans产生的向量叉积，比如30%
        return (int)(0.3 * cardinality1 * cardinality2);
    }


    public List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats, Map<String, Double> filterSelectivityMap, boolean explain) throws ParseException {
        int size = joins.size();
        //记录最优查询计划
        PlanCache planCache = new PlanCache();
        CostCard bestCostCard = null;
        for(int i=1;i<=size;i++){
            //得到固定长度i的子集，并遍历每一个子集
            for(Set<LogicalJoinNode> s: enumerateSubsets(joins,i)){
                //遍历集合中的集合，得到集合中的每个集合的最小代价
                double bestCost = Double.MAX_VALUE;
                bestCostCard = new CostCard();
                for(LogicalJoinNode logicalJoinNode:s){
                    //计算 logicalJoinNode 与 其他node(s中的其它node)的join 结果
                    CostCard costCard = computeCostAndCardOfSubplan(stats, filterSelectivityMap, logicalJoinNode, s, bestCost, planCache);
                    if(costCard==null){
                        continue;
                    }
                    if(costCard.cost<bestCost){
                        bestCost = costCard.cost;
                        bestCostCard = costCard;
                    }
                }
                planCache.addPlan(s,bestCost,bestCostCard.card,bestCostCard.plan);
            }
        }
        //是否解释其查询计划
        if(explain){
            assert bestCostCard!=null;
            printJoins(bestCostCard.plan,planCache,stats,filterSelectivityMap);
        }
        assert bestCostCard!=null;
        return bestCostCard.plan;
    }

    /**
     * 生成固定长度子集
     * @param v
     * @param size
     * @param <T>
     * @return
     */
    private <T> Set<Set<T>>  enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> els = new HashSet<>();
        List<Boolean> used = new ArrayList<>();
        //初始化used集合
        for(int i=0;i<v.size();i++){
            used.add(false);
        }
        enumerateSubsetsDFS(els,v,used,size,0,0);
        return els;
    }

    public <T> void enumerateSubsetsDFS(Set<Set<T>> els,List<T> v,List<Boolean> used,int size,int count,int next){
        if(count==size){
            Set<T> res = new HashSet<>();
            for(int i=0;i<v.size();i++){
                if(used.get(i)){
                    res.add(v.get(i));
                }
            }
            els.add(res);
            return;
        }
        for(int i=next;i<v.size()-(size-count-1);i++){
            used.set(i,true);
            enumerateSubsetsDFS(els,v,used,size,count+1,i+1);
            used.set(i,false);
        }

    }

    private CostCard computeCostAndCardOfSubplan(Map<String, TableStats> stats,
                                                 Map<String, Double> filterSelectivityMap,
                                                 LogicalJoinNode joinToRemove,
                                                 Set<LogicalJoinNode> joinSet,
                                                 double bestCostSoFar,
                                                 PlanCache pc) throws ParseException {
        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParseException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParseException("Unknown table " + j.t2Alias);

        //1. 获取joinToRemove节点的基本信息
        String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        //2. 生成一个连接方案
        if (news.isEmpty()) {
            //2.1 当只有一个joinToRemove节点时，就利用该节点来计算cost和card
            prevBest = new ArrayList<>();

            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivityMap.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivityMap.get(j.t2Alias));
            rightPkey = table2Alias != null && isPkey(table2Alias, j.f2PureName);

        } else {
            //2.2 当有多个节点时，先获取删除了joinToRemove节点后的joinSet的子最佳方案
            prevBest = pc.getOrder(news);
            if (prevBest == null) {
                return null;
            }

            //获取删除了joinToRemove节点后的joinSet的子最佳方案的cost和card
            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // 然后再生成joinToRemove节点的表与子最佳方案进行连接的方案
            if (doesJoin(prevBest, table1Alias)) {
                //当joinToRemove节点左表在子最佳方案中，左表=子最佳方案，右表=joinToRemove节点右表
                t1cost = prevBestCost;
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivityMap.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);

            } else if (doesJoin(prevBest, j.t2Alias)) {
                //当joinToRemove节点右表在子最佳方案中，左表=joinToRemove节点左表，右表=子最佳方案
                t2cost = prevBestCost;
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);

                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivityMap.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // joinToRemove节点左右表均不在子最佳方案中
                return null;
            }
        }

        //3. 计算当前连接方案的cost
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        //4. 交换一次join两边顺序，再计算cost，并比较两次的cost得到最佳方案
        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        //5. 生成最终结果
        CostCard cc = new CostCard();
        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey, rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j);

        return cc;
    }

    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table) || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * 解释查询计划
     * @param js
     * @param pc
     * @param stats
     * @param selectivities
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc,
                            Map<String, TableStats> stats,
                            Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.getCost(pathSoFar) + ", card = "
                    + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) {
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                        selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) {
                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                + " (Cost = "
                                + stats.get(table2Name)
                                .estimateScanCost()
                                + ", card = "
                                + stats.get(table2Name)
                                .estimateTableCardinality(
                                        selectivities
                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }



}
