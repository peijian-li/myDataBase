package optimizer;

import common.*;
import execution.Predicate;
import lombok.Getter;
import storage.*;
import transaction.TransactionId;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TableStats {

    @Getter
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();
    private static final int IO_COST_PER_PAGE = 1000;
    private static final int NUM_HIST_BINS = 100;

    private int tableId;
    private int ioCostPerPage;
    private BufferPool bufferPool = Database.getBufferPool();
    private Catalog catalog = Database.getCatalog();
    private TupleDesc tupleDesc;
    private DbFileIterator dbFileIterator;  //所有tuple集合迭代器
    private int pageNum;
    private int total = 0;
    private int[] max;  //每一个字段的max
    private int[] min;  //每一个字段的min
    private IntHistogram[] intHistograms;  //每一个字段的直方图

    public TableStats(int tableId, int ioCostPerPage) {
        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;
        HeapFile heapFile = (HeapFile) catalog.getDatabaseFile(tableId);
        this.tupleDesc  = heapFile.getTupleDesc();
        this.pageNum = heapFile.numPages();
        this.dbFileIterator = heapFile.iterator(new TransactionId());
        this.max  = new int[tupleDesc.numFields()];
        this.min = new int[tupleDesc.numFields()];
        this.intHistograms = new IntHistogram[tupleDesc.numFields()];
        Arrays.fill(min,Integer.MAX_VALUE);
        Arrays.fill(max,Integer.MIN_VALUE);

        //计算表中每个int字段的max，min
        try {
            this.dbFileIterator.open();
            while(dbFileIterator.hasNext()){
                this.total++;
                Tuple tuple = dbFileIterator.next();
                for(int i=0;i<max.length;i++){
                    Type fieldType = tuple.getField(i).getType();
                    if(fieldType.equals(Type.INT_TYPE)){
                        IntField field = (IntField)tuple.getField(i);
                        int value = field.getValue();
                        max[i]=Math.max(max[i],value);
                        min[i]=Math.min(min[i],value);
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        //生成每个int字段直方图
        for(int i=0;i<tupleDesc.numFields();i++){
            Type fieldType = tupleDesc.getFieldType(i);
            if(fieldType.equals(Type.STRING_TYPE)){
                continue;
            }
            this.intHistograms[i]  =new IntHistogram(100,min[i],max[i]);
            try {
                this.dbFileIterator.rewind();
                while(dbFileIterator.hasNext()){
                    Tuple tuple = dbFileIterator.next();
                    IntField field = (IntField)tuple.getField(i);
                    this.intHistograms[i].addValue(field.getValue());
                }
            } catch (DbException | TransactionAbortedException e) {
                e.printStackTrace();
            }

        }

        this.dbFileIterator.close();
    }

    public static TableStats getTableStats(String tableName) {
        return statsMap.get(tableName);
    }

    public static void setTableStats(String tableName, TableStats stats) {
        statsMap.put(tableName, stats);
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableId = tableIt.next();
            TableStats s = new TableStats(tableId, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableId), s);
        }
        System.out.println("Done.");
    }

    /**
     * 估算扫描代价，page数*每一页io代价
     * @return
     */
    public double estimateScanCost() {
        return this.pageNum*ioCostPerPage;
    }

    /**
     * 估算基数，tuple数*选择因子
     * @param selectivityFactor
     * @return
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(totalTuples()*selectivityFactor);
    }

    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     *估算某一个字段选择性
     * @param field
     * @param op
     * @param constant
     * @return
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Type type = constant.getType();
        if(type.equals(Type.STRING_TYPE)){
            StringHistogram stringHistogram = new StringHistogram(100);
            return stringHistogram.estimateSelectivity(op,((StringField)constant).getValue());
        }else if(type.equals(Type.INT_TYPE)){
            return this.intHistograms[field].estimateSelectivity(op,((IntField)constant).getValue());
        }
        return -1.0;
    }

    public int totalTuples() {
        return total;
    }


}
