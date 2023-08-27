package execution;

import common.DbException;
import common.TransactionAbortedException;
import optimizer.LogicalPlan;
import storage.Tuple;
import storage.TupleDesc;
import transaction.TransactionId;

import java.io.Serializable;
import java.util.NoSuchElementException;

public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    transient private OpIterator physicalPlan;
    transient private LogicalPlan logicalPlan;
    transient private boolean started = false;

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan lp) {
        this.logicalPlan = lp;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(OpIterator physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    public OpIterator getPhysicalPlan() {
        return this.physicalPlan;
    }

    public Query(TransactionId tid) {
        this.tid=tid;
    }


    public void start() throws DbException, TransactionAbortedException {
        physicalPlan.open();
        started = true;
    }

    public TupleDesc getOutputTupleDesc() {
        return this.physicalPlan.getTupleDesc();
    }


    public boolean hasNext() throws DbException, TransactionAbortedException {
        return physicalPlan.hasNext();
    }


    public Tuple next() throws DbException, NoSuchElementException, TransactionAbortedException {
        if (!started)
            throw new DbException("Database not started.");

        return physicalPlan.next();
    }

    public void close() {
        physicalPlan.close();
        started = false;
    }

    public void execute() throws DbException, TransactionAbortedException {
        //打印tuple描述
        TupleDesc td = this.getOutputTupleDesc();
        StringBuilder names = new StringBuilder();
        for (int i = 0; i < td.numFields(); i++) {
            names.append(td.getFieldName(i)).append("\t");
        }
        System.out.println(names);

        //打印分割线
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            System.out.print("-");
        }
        System.out.println();

        //打印tuple集合
        this.start();
        int cnt = 0;
        while (this.hasNext()) {
            Tuple tup = this.next();
            System.out.println(tup);
            cnt++;
        }

        System.out.println("\n " + cnt + " rows.");
        this.close();
    }
}
