import Zql.*;
import Zql.ParseException;
import common.*;
import execution.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import optimizer.LogicalPlan;
import optimizer.TableStats;
import storage.*;
import transaction.Transaction;
import transaction.TransactionId;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

@Slf4j
public class Server {
    private static boolean explain = false;
    @Setter
    private Transaction currentTransaction = null;//当前事务

    public static void main (String[] args) throws IOException {




        Server server = new Server();
        server.processNextStatement("SELECT * FROM student WHERE c1 = 1;");
    }


    private void processNextStatement(String sql) {
        ByteArrayInputStream is = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8));
        try {
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();
            Query query = null;
            if (s instanceof ZTransactStmt)
                handleTransactStatement((ZTransactStmt) s);
            else {
                if (currentTransaction==null) {
                    currentTransaction = new Transaction();
                    currentTransaction.start();
                    System.out.println("Started a new transaction tid = " + currentTransaction.getId().getId());
                }
                try {
                    if (s instanceof ZInsert)
                        query = handleInsertStatement((ZInsert) s, currentTransaction.getId());
                    else if (s instanceof ZDelete)
                        query = handleDeleteStatement((ZDelete) s, currentTransaction.getId());
                    else if (s instanceof ZQuery)
                        query = handleQueryStatement((ZQuery) s, currentTransaction.getId());
                    else if (s instanceof ZUpdate)
                        query = handleUpdateStatement((ZUpdate) s, currentTransaction.getId());
                    else {
                        System.out.println("Can't parse " + s + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
                    }

                    if (query != null)
                        query.execute();

                    if ( currentTransaction != null) {
                        currentTransaction.commit();
                        System.out.println("Transaction " + currentTransaction.getId().getId() + " committed.");
                    }
                } catch (Throwable a) {
                    if (currentTransaction != null) {
                        currentTransaction.abort();
                        System.out.println("Transaction " + currentTransaction.getId().getId() + " aborted because of unhandled error");
                    }

                    if (a instanceof common.ParseException || a instanceof Zql.ParseException)
                        throw new common.ParseException((Exception) a);
                    if (a instanceof Zql.TokenMgrError)
                        throw (Zql.TokenMgrError) a;
                    throw new DbException(a.getMessage());
                } finally {
                    if (currentTransaction!=null&&!currentTransaction.started)
                        currentTransaction = null;
                }
            }

        } catch (IOException | DbException e) {
            e.printStackTrace();
        } catch (common.ParseException e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (ParseException | TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        }
    }

    private void handleTransactStatement(ZTransactStmt s) throws IOException, common.ParseException {
        switch (s.getStmtType()) {
            case "COMMIT":
                if (currentTransaction == null)
                    throw new common.ParseException(
                            "No transaction is currently running");
                currentTransaction.commit();
                currentTransaction = null;
                System.out.println("Transaction " + currentTransaction.getId().getId() + " committed.");
                break;
            case "ROLLBACK":
                if (currentTransaction == null)
                    throw new common.ParseException("No transaction is currently running");
                currentTransaction.abort();
                currentTransaction = null;
                System.out.println("Transaction " + currentTransaction.getId().getId() + " aborted.");
                break;
            case "SET TRANSACTION":
                if (currentTransaction != null)
                    throw new common.ParseException("Can't start new transactions until current transaction has been committed or rolledback.");
                currentTransaction = new Transaction();
                currentTransaction.start();
                System.out.println("Started a new transaction tid = " + currentTransaction.getId().getId());
                break;
            default:
                throw new common.ParseException("Unsupported operation");
        }
    }

    private Query handleInsertStatement(ZInsert s, TransactionId tId) throws DbException, IOException, common.ParseException, Zql.ParseException {
        int tableId;
        try {
            tableId = Database.getCatalog().getTableId(s.getTable());
        } catch (NoSuchElementException e) {
            throw new common.ParseException("Unknown table : " + s.getTable());
        }

        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);

        Tuple t = new Tuple(td);
        int i = 0;
        OpIterator newTuples;

        if (s.getValues() != null) {
            List<ZExp> values = s.getValues();
            if (td.numFields() != values.size()) {
                throw new common.ParseException("INSERT statement does not contain same number of fields as table " + s.getTable());
            }
            for (ZExp e : values) {
                if (!(e instanceof ZConstant))
                    throw new common.ParseException("Complex expressions not allowed in INSERT statements.");
                ZConstant zc = (ZConstant) e;
                if (zc.getType() == ZConstant.NUMBER) {
                    if (td.getFieldType(i) != Type.INT_TYPE) {
                        throw new common.ParseException("Value " + zc.getValue() + " is not an integer, expected a string.");
                    }
                    IntField f = new IntField(new Integer(zc.getValue()));
                    t.setField(i, f);
                } else if (zc.getType() == ZConstant.STRING) {
                    if (td.getFieldType(i) != Type.STRING_TYPE) {
                        throw new common.ParseException("Value " + zc.getValue() + " is a string, expected an integer.");
                    }
                    StringField f = new StringField(zc.getValue(), Type.STRING_LEN);
                    t.setField(i, f);
                } else {
                    throw new common.ParseException("Only string or int fields are supported.");
                }
                i++;
            }
            List<Tuple> tuples = new ArrayList<>();
            tuples.add(t);
            newTuples = new TupleArrayIterator(tuples);
        } else {
            ZQuery zq = s.getQuery();
            LogicalPlan lp = parseQueryLogicalPlan(tId, zq);
            newTuples = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);
        }
        Query query = new Query(tId);
        query.setPhysicalPlan(new Insert(tId, newTuples, tableId));
        return query;
    }

    private Query handleDeleteStatement(ZDelete s, TransactionId tid) throws common.ParseException, IOException, ParseException {
        int id;
        try {
            id = Database.getCatalog().getTableId(s.getTable());
        } catch (NoSuchElementException e) {
            throw new common.ParseException("Unknown table : " + s.getTable());
        }
        String name = s.getTable();
        Query query = new Query(tid);
        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(s.toString());
        lp.addScan(id, name);
        if (s.getWhere() != null)
            processExpression(tid, (ZExpression) s.getWhere(), lp);
        lp.addProjectField("null.*", null);
        OpIterator op = new Delete(tid, lp.physicalPlan(tid, TableStats.getStatsMap(), false));
        query.setPhysicalPlan(op);
        return query;

    }

    private Query handleUpdateStatement(ZUpdate s, TransactionId tid){
        return null;
    }


    private Query handleQueryStatement(ZQuery s, TransactionId tId) throws IOException, common.ParseException, Zql.ParseException {
        Query query = new Query(tId);
        LogicalPlan lp = parseQueryLogicalPlan(tId, s);
        OpIterator physicalPlan = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);
        query.setPhysicalPlan(physicalPlan);
        query.setLogicalPlan(lp);
        return query;
    }

    /**
     * 生成查询计划
     * @param tid
     * @param q
     * @return
     * @throws IOException
     * @throws Zql.ParseException
     * @throws common.ParseException
     */
    private LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery q) throws IOException, Zql.ParseException, common.ParseException {
        LogicalPlan lp = new LogicalPlan();

        //初始化query
        lp.setQuery(q.toString());

        //解析query中from，初始化tables,tableMap
        List<ZFromItem> from = q.getFrom();
        for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.get(i);
            try {
                int id = Database.getCatalog().getTableId(fromIt.getTable());
                String name;
                if (fromIt.getAlias() != null) {
                    name = fromIt.getAlias();
                } else {
                    name = fromIt.getTable();
                }
                lp.addScan(id, name);
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                throw new common.ParseException("Table " + fromIt.getTable() + " is not in catalog");
            }
        }

        //解析query中where,初始化joins和filters
        ZExp w = q.getWhere();
        if (w != null) {
            if (!(w instanceof ZExpression)) {
                throw new common.ParseException("Nested queries are currently unsupported.");
            }
            ZExpression wx = (ZExpression) w;
            processExpression(tid, wx, lp);
        }

        //解析query中groupby
        ZGroupBy groupBy = q.getGroupBy();
        String groupByField = null;
        if (groupBy != null) {
            List<ZExp> gbs = groupBy.getGroupBy();
            if (gbs.size() > 1) {
                throw new common.ParseException("不支持对多个字段分组");
            }
            if (gbs.size() == 1) {
                ZExp gbe = gbs.get(0);
                if (!(gbe instanceof ZConstant)) {
                    throw new common.ParseException("Complex grouping expressions (" + gbe + ") not supported.");
                }
                groupByField = ((ZConstant) gbe).getValue();
                log.info("GROUP BY FIELD : " + groupByField);
            }

        }

        //解析query中select，初始化selectList
        List<ZSelectItem> selectList = q.getSelect();
        String aggregateField = null;
        String aggregateOperator = null;
        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem select = selectList.get(i);
            if (select.getAggregate() == null && (select.isExpression() && !(select.getExpression() instanceof ZConstant))) {
                throw new common.ParseException("Expressions in SELECT list are not supported.");
            }
            if (select.getAggregate() != null) {
                if (aggregateField != null) {
                    throw new common.ParseException("不支持对多个字段聚合");
                }
                aggregateField = ((ZConstant) ((ZExpression) select.getExpression()).getOperand(0)).getValue();
                aggregateOperator = select.getAggregate();
                System.out.println("Aggregate field is " + aggregateField + ", agg fun is : " + aggregateOperator);
                lp.addProjectField(aggregateField, aggregateOperator);
            } else {
                //有分组字段时，非聚合字段必须为分组字段
                if (groupByField != null && !(groupByField.equals(select.getTable() + "." + select.getColumn()) || groupByField.equals(select.getColumn()))) {
                    throw new common.ParseException("Non-aggregate field " + select.getColumn() + " does not appear in GROUP BY list.");
                }
                lp.addProjectField(select.getTable() + "." + select.getColumn(), null);
            }
        }

        if (groupByField != null && aggregateOperator == null) {
            throw new common.ParseException("分组而不进行聚合");
        }

        //初始化聚合分组相关字段
        if (aggregateOperator != null) {
            lp.addAggregate(aggregateOperator, aggregateField, groupByField);
        }

        //解析query中orderBy，初始化排序相关字段
        if (q.getOrderBy() != null) {
            List<ZOrderBy> orderBy = q.getOrderBy();
            if (orderBy.size() > 1) {
                throw new common.ParseException("不支持对多个字段排序");
            }
            ZOrderBy oby = orderBy.get(0);
            if (!(oby.getExpression() instanceof ZConstant)) {
                throw new common.ParseException("Complex ORDER BY's are not supported");
            }
            ZConstant f = (ZConstant) oby.getExpression();
            lp.addOrderBy(f.getValue(), oby.getAscOrder());
        }
        return lp;
    }


    private void processExpression(TransactionId tid, ZExpression wx, LogicalPlan lp) throws common.ParseException, IOException, ParseException {
        if (wx.getOperator().equals("AND")) {
            //递归处理and
            for (int i = 0; i < wx.nbOperands(); i++) {
                if (!(wx.getOperand(i) instanceof ZExpression)) {
                    throw new common.ParseException("不支持嵌套查询");
                }
                ZExpression newWx = (ZExpression) wx.getOperand(i);
                processExpression(tid, newWx, lp);
            }
        } else if (wx.getOperator().equals("OR")) {
            //不支持or
            throw new common.ParseException("不支持or表达式");
        } else {
            //处理> = <等操作符
            List<ZExp> ops = wx.getOperands();
            if (ops.size() != 2) {
                throw new common.ParseException("只支持简单二元表达式");
            }
            boolean isJoin;//是否联表
            Predicate.Op op = getOp(wx.getOperator());
            boolean op1const = ops.get(0) instanceof ZConstant;
            boolean op2const = ops.get(1) instanceof ZConstant;
            if (op1const && op2const) {
                //俩操作数均为常量时
                isJoin = ((ZConstant) ops.get(0)).getType() == ZConstant.COLUMNNAME && ((ZConstant) ops.get(1)).getType() == ZConstant.COLUMNNAME;
            } else if (ops.get(0) instanceof ZQuery || ops.get(1) instanceof ZQuery) {
                //俩操作数有一个为查询语句时
                isJoin = true;
            } else if (ops.get(0) instanceof ZExpression || ops.get(1) instanceof ZExpression) {
                throw new common.ParseException("操作数只支持字段、常量或子查询");
            } else {
                isJoin = false;
            }

            if (isJoin) {
                //存在联表
                String tab1field , tab2field ;
                if (!op1const) {
                    tab1field="";
                } else {
                    tab1field = ((ZConstant) ops.get(0)).getValue();
                }
                if (!op2const) {
                    LogicalPlan subLogicalPlan = parseQueryLogicalPlan(tid, (ZQuery) ops.get(1));
                    OpIterator physicalPlan = subLogicalPlan.physicalPlan(tid, TableStats.getStatsMap(), explain);
                    lp.addJoin(tab1field, physicalPlan, op);
                } else {
                    tab2field = ((ZConstant) ops.get(1)).getValue();
                    lp.addJoin(tab1field, tab2field, op);
                }

            } else {
                //不存在联表
                String column;
                String compValue;
                ZConstant op1 = (ZConstant) ops.get(0);
                ZConstant op2 = (ZConstant) ops.get(1);
                if (op1.getType() == ZConstant.COLUMNNAME) {
                    column = op1.getValue();
                    compValue = op2.getValue();
                } else {
                    column = op2.getValue();
                    compValue = op1.getValue();
                }
                lp.addFilter(column, op, compValue);
            }
        }

    }

    private static Predicate.Op getOp(String s) throws common.ParseException {
        if (s.equals("="))
            return Predicate.Op.EQUALS;
        if (s.equals(">"))
            return Predicate.Op.GREATER_THAN;
        if (s.equals(">="))
            return Predicate.Op.GREATER_THAN_OR_EQ;
        if (s.equals("<"))
            return Predicate.Op.LESS_THAN;
        if (s.equals("<="))
            return Predicate.Op.LESS_THAN_OR_EQ;
        if (s.equals("LIKE"))
            return Predicate.Op.LIKE;
        if (s.equals("~"))
            return Predicate.Op.LIKE;
        if (s.equals("<>"))
            return Predicate.Op.NOT_EQUALS;
        if (s.equals("!="))
            return Predicate.Op.NOT_EQUALS;

        throw new common.ParseException("Unknown predicate " + s);
    }




}
