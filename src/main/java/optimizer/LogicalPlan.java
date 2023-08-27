package optimizer;


import common.Database;
import common.ParseException;
import common.Type;
import execution.*;
import lombok.Getter;
import lombok.Setter;
import storage.Field;
import storage.IntField;
import storage.StringField;
import storage.TupleDesc;
import transaction.TransactionId;

import java.util.*;

public class LogicalPlan {
    private List<LogicalJoinNode> joins;//join节点集合，表示join顺序
    private final List<LogicalFilterNode> filters;//过滤器集合

    private final List<LogicalScanNode> tables;//表集合
    private final Map<String,Integer> tableMap;//key:表名 value:表id

    private final Map<String, OpIterator> tableIteratorMap;//key:表名 value:表迭代器

    private final List<LogicalSelectListNode> selectList;//select节点集合，表示查询的字段

    private boolean hasAggregate = false;//是否聚合
    private String aggregateOperator;//聚合操作符
    private String aggregateField;//聚合字段
    private String groupByField = null;//分组字段


    private boolean hasOrderBy = false;//是否排序
    private boolean oderByAscent=false;//排序是否上升
    private String oderByField;//排序字段

    @Getter
    @Setter
    private String query;//查询语句

    public LogicalPlan() {
        joins = new ArrayList<>();
        filters = new ArrayList<>();
        tables = new ArrayList<>();
        tableIteratorMap = new HashMap<>();
        tableMap = new HashMap<>();
        selectList = new ArrayList<>();
        this.query = "";
    }

    public OpIterator physicalPlan(TransactionId t, Map<String,TableStats> baseTableStats, boolean explain) throws ParseException {
        Iterator<LogicalScanNode> tableIt = tables.iterator();
        Map<String,TableStats> statsMap = new HashMap<>();//key:表名 value:表数据统计
        Map<String,Double> selectivityMap = new HashMap<>();//key:表名 value:表选择性
        Map<String,String> equivMap = new HashMap<>();//key: value:

        //遍历表集合
        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            SeqScan seqScan;
            try {
                seqScan = new SeqScan(t, Database.getCatalog().getDatabaseFile(table.id).getId(), table.name);
            } catch (NoSuchElementException e) {
                throw new ParseException("Unknown table " + table.id);
            }
            tableIteratorMap.put(table.name,seqScan);
            String baseTableName = Database.getCatalog().getTableName(table.id);
            statsMap.put(baseTableName, baseTableStats.get(baseTableName));
            selectivityMap.put(table.name, 1.0);
        }

        //遍历过滤器集合
        for (LogicalFilterNode logicalFilterNode : filters) {
            OpIterator tableIterator = tableIteratorMap.get(logicalFilterNode.table);
            if (tableIterator == null) {
                throw new ParseException("Unknown table in WHERE clause " + logicalFilterNode.table);
            }

            //filterNode转化为filter
            Field f;
            Type fieldType;
            TupleDesc tupleDesc = tableIteratorMap.get(logicalFilterNode.table).getTupleDesc();
            try {
                fieldType = tupleDesc.getFieldType(tupleDesc.fieldNameToIndex(logicalFilterNode.fieldQuantifiedName));
            } catch (NoSuchElementException e) {
                throw new ParseException("Unknown field in filter expression " + logicalFilterNode.fieldQuantifiedName);
            }
            if (fieldType == Type.INT_TYPE) {
                f = new IntField(new Integer(logicalFilterNode.constant));
            } else {
                f = new StringField(logicalFilterNode.constant, Type.STRING_LEN);
            }

            Predicate predicate ;
            try {
                predicate = new Predicate(tableIterator.getTupleDesc().fieldNameToIndex(logicalFilterNode.fieldQuantifiedName), logicalFilterNode.operator, f);
            } catch (NoSuchElementException e) {
                throw new ParseException("Unknown field " + logicalFilterNode.fieldQuantifiedName);
            }
            tableIteratorMap.put(logicalFilterNode.table, new Filter(predicate, tableIterator));

            //计算表选择性
            TableStats s = statsMap.get(Database.getCatalog().getTableName(this.getTableId(logicalFilterNode.table)));
            double sel = s.estimateSelectivity(tableIterator.getTupleDesc().fieldNameToIndex(logicalFilterNode.fieldQuantifiedName), logicalFilterNode.operator, f);
            selectivityMap.put(logicalFilterNode.table, selectivityMap.get(logicalFilterNode.table) * sel);
        }

        //join优化
        JoinOptimizer jo = new JoinOptimizer(this,joins);
        joins = jo.orderJoins(statsMap,selectivityMap,explain);
        for (LogicalJoinNode lj : joins) {
            OpIterator plan1;
            OpIterator plan2;
            boolean isSubQueryJoin = lj instanceof LogicalSubplanJoinNode;
            String t1name, t2name;

            if (equivMap.get(lj.t1Alias) != null)
                t1name = equivMap.get(lj.t1Alias);
            else
                t1name = lj.t1Alias;

            if (equivMap.get(lj.t2Alias) != null)
                t2name = equivMap.get(lj.t2Alias);
            else
                t2name = lj.t2Alias;

            plan1 = tableIteratorMap.get(t1name);

            if (isSubQueryJoin) {
                plan2 = ((LogicalSubplanJoinNode) lj).subPlan;
                if (plan2 == null)
                    throw new ParseException("Invalid subquery.");
            } else {
                plan2 = tableIteratorMap.get(t2name);
            }

            if (plan1 == null)
                throw new ParseException("Unknown table in WHERE clause " + lj.t1Alias);
            if (plan2 == null)
                throw new ParseException("Unknown table in WHERE clause " + lj.t2Alias);

            OpIterator j;
            j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);
            tableIteratorMap.put(t1name, j);

            if (!isSubQueryJoin) {
                tableIteratorMap.remove(t2name);
                equivMap.put(t2name, t1name);
                for (Map.Entry<String, String> s : equivMap.entrySet()) {
                    String val = s.getValue();
                    if (val.equals(t2name)) {
                        s.setValue(t1name);
                    }
                }

            }

        }

        if (tableIteratorMap.size() > 1) {
            throw new ParseException("Query does not include join expressions joining all nodes!");
        }

        //
        OpIterator node = tableIteratorMap.entrySet().iterator().next().getValue();
        List<Integer> outFields = new ArrayList<>();
        List<Type> outTypes = new ArrayList<>();
        for (int i = 0; i < selectList.size(); i++) {
            LogicalSelectListNode si = selectList.get(i);
            if (si.aggregateOperator != null) {
                outFields.add(groupByField!=null?1:0);
                TupleDesc tupleDesc = node.getTupleDesc();
                try {
                    tupleDesc.fieldNameToIndex(si.fieldName);
                } catch (NoSuchElementException e) {
                    throw new ParseException("Unknown field " +  si.fieldName + " in SELECT list");
                }
                outTypes.add(Type.INT_TYPE);  //the type of all aggregate functions is INT

            } else if (hasAggregate) {
                if (groupByField == null) {
                    throw new ParseException("Field " + si.fieldName + " does not appear in GROUP BY list");
                }
                outFields.add(0);
                TupleDesc tupleDesc = node.getTupleDesc();
                int  id;
                try {
                    id = tupleDesc.fieldNameToIndex(groupByField);
                } catch (NoSuchElementException e) {
                    throw new ParseException("Unknown field " +  groupByField + " in GROUP BY statement");
                }
                outTypes.add(tupleDesc.getFieldType(id));
            } else if (si.fieldName.equals("null.*")) {
                TupleDesc tupleDesc = node.getTupleDesc();
                for ( i = 0; i < tupleDesc.numFields(); i++) {
                    outFields.add(i);
                    outTypes.add(tupleDesc.getFieldType(i));
                }
            } else  {
                TupleDesc tupleDesc = node.getTupleDesc();
                int id;
                try {
                    id = tupleDesc.fieldNameToIndex(si.fieldName);
                } catch (NoSuchElementException e) {
                    throw new ParseException("Unknown field " +  si.fieldName + " in SELECT list");
                }
                outFields.add(id);
                outTypes.add(tupleDesc.getFieldType(id));

            }
        }


        //
        if (hasAggregate) {
            TupleDesc tupleDesc = node.getTupleDesc();
            Aggregate aggNode;
            try {
                aggNode = new Aggregate(node,
                        tupleDesc.fieldNameToIndex(aggregateField),
                        groupByField == null? Aggregator.NO_GROUPING:tupleDesc.fieldNameToIndex(groupByField),
                        getAggOp(aggregateOperator));
            } catch (NoSuchElementException | IllegalArgumentException e) {
                throw new ParseException(e);
            }
            node = aggNode;
        }

        //
        if (hasOrderBy) {
            node = new OrderBy(node.getTupleDesc().fieldNameToIndex(oderByField), oderByAscent, node);
        }

        return new Project(outFields, outTypes, node);
    }

    private  Aggregator.Op getAggOp(String s) throws ParseException{
        s = s.toUpperCase();
        if (s.equals("AVG")) return Aggregator.Op.AVG;
        if (s.equals("SUM")) return Aggregator.Op.SUM;
        if (s.equals("COUNT")) return Aggregator.Op.COUNT;
        if (s.equals("MIN")) return Aggregator.Op.MIN;
        if (s.equals("MAX")) return Aggregator.Op.MAX;
        throw new ParseException("Unknown predicate " + s);
    }

    public Integer getTableId(String alias) {
        return tableMap.get(alias);
    }

    public Map<String,Integer> getTableAliasToIdMapping()
    {
        return this.tableMap;
    }

    public void addFilter(String field, Predicate.Op operator, String constantValue) throws ParseException{
        field = disambiguateName(field);
        String table = field.split("[.]")[0];
        LogicalFilterNode logicalFilterNode = new LogicalFilterNode(table, field.split("[.]")[1], operator, constantValue);
        filters.add(logicalFilterNode);
    }

    /**
     * 添加join节点
     * @param joinField1
     * @param joinField2
     * @param pred
     * @throws ParseException
     */
    public void addJoin( String joinField1, String joinField2, Predicate.Op pred) throws ParseException {
        joinField1 = disambiguateName(joinField1);
        joinField2 = disambiguateName(joinField2);
        String table1Alias = joinField1.split("[.]")[0];
        String table2Alias = joinField2.split("[.]")[0];
        String pureField1 = joinField1.split("[.]")[1];
        String pureField2 = joinField2.split("[.]")[1];
        if (table1Alias.equals(table2Alias))
            throw new ParseException("Cannot join on two fields from same table");
        LogicalJoinNode lj = new LogicalJoinNode(table1Alias,table2Alias,pureField1, pureField2, pred);
        System.out.println("Added join between " + joinField1 + " and " + joinField2);
        joins.add(lj);

    }

    /**
     * 添加子查询join节点
     * @param joinField1
     * @param joinField2
     * @param pred
     * @throws ParseException
     */
    public void addJoin(String joinField1, OpIterator joinField2, Predicate.Op pred) throws ParseException {
        joinField1 = disambiguateName(joinField1);
        String table1 = joinField1.split("[.]")[0];
        String pureField = joinField1.split("[.]")[1];
        LogicalSubplanJoinNode lj = new LogicalSubplanJoinNode(table1,pureField, joinField2, pred);
        System.out.println("Added subQuery join on " + joinField1);
        joins.add(lj);
    }



    public void addScan(int id, String name) {
        System.out.println("Added scan of table " + name);
        tables.add(new LogicalScanNode(id,name));
        tableMap.put(name,id);
    }


    public void addProjectField(String fieldName, String aggregateOperator) throws ParseException {
        fieldName=disambiguateName(fieldName);
        if (fieldName.equals("*")) {
            fieldName = "null.*";
        }
        System.out.println("Added select list field " + fieldName);
        if (aggregateOperator != null) {
            System.out.println("\t with aggregator " + aggregateOperator);
        }
        selectList.add(new LogicalSelectListNode(aggregateOperator, fieldName));
    }

    public void addAggregate(String aggregateOperator, String aggregateField, String groupByField) throws ParseException {
        aggregateField=disambiguateName(aggregateField);
        if (groupByField!=null) {
            groupByField = disambiguateName(groupByField);
        }
        this.aggregateOperator = aggregateOperator;
        this.aggregateField = aggregateField;
        this.groupByField = groupByField;
        hasAggregate = true;
    }

    public void addOrderBy(String field, boolean asc) throws ParseException {
        field=disambiguateName(field);
        oderByField = field;
        oderByAscent = asc;
        hasOrderBy = true;
    }

    /**
     * 将字段名转换为（表名.字段名）的形式，消除歧义
     * @param name
     * @return
     * @throws ParseException
     */
    private String disambiguateName(String name) throws ParseException {
        String[] fields = name.split("[.]");
        if (fields.length == 2 && (!fields[0].equals("null")))
            return name;
        if (fields.length > 2)
            throw new ParseException("Field " + name + " is not a valid field reference.");
        if (fields.length == 2)
            name = fields[1];
        if (name.equals("*"))
            return name;
        Iterator<LogicalScanNode> tableIt = tables.iterator();
        String tableName = null;
        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            try {
                TupleDesc tupleDesc = Database.getCatalog().getDatabaseFile(table.id).getTupleDesc();
                tupleDesc.fieldNameToIndex(name);
                if (tableName == null) {
                    tableName = table.name;
                } else {
                    throw new ParseException("Field " + name + " appears in multiple tables; disambiguate by referring to it as tablename." + name);
                }
            } catch (NoSuchElementException e) {
            }
        }
        if (tableName != null)
            return tableName + "." + name;
        else
            throw new ParseException("Field " + name + " does not appear in any tables.");

    }







}
