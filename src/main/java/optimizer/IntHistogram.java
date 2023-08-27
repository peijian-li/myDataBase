package optimizer;

import execution.Predicate;
import lombok.ToString;

@ToString
public class IntHistogram {

    private int bucketNum;  //桶数量
    private int min;    //当前field最小值
    private int max; //当前field最大值
    private double avg;  //平均每个桶表示的值数量
    private Bucket[] buckets;  //每一个桶
    private int tupleNum;  //tuple数量

    @ToString
    public class Bucket{
        private double  left;    //当前bucket左边界
        private double right;    //当前bucket右边界
        private double w;      //当前bucket宽度
        private int count;     //当前bucket的tuple个数

        public Bucket(double left,double right){
            this.left = left;
            this.right = right;
            this.w = right - left;
            this.count = 0;
        }

    }

    public IntHistogram(int bucketNum, int min, int max) {
        this.bucketNum=bucketNum;
        this.min=min;
        this.max = max;
        this.avg = Math.ceil((double) (max - min) / bucketNum);
        this.buckets = new Bucket[bucketNum];
        this.tupleNum = 0;
        double l = min;
        for(int i=0;i<bucketNum;i++){
            buckets[i] = new Bucket(l,l+this.avg);
            l = l + avg;
        }
    }

    public void addValue(int v) {
        int target = binarySearch(v);
        if(target!=bucketNum){
            buckets[target].count++;
            tupleNum++;
        }
    }

    private int binarySearch(int v){
        int l = 0;
        int r = bucketNum;
        while(l<r){
            int mid=(l+r)/2;
            if(buckets[mid].right>v){
                r=mid;
            }else{
                l=mid+1;
            }
        }
        return l;
    }

    /**
     * 估算选择性，满足条件tuple数/全部tuple数
     * @param op
     * @param v
     * @return
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int target = binarySearch(v);
        Bucket cur;
        if(target!=bucketNum){
            cur = buckets[target];
        }else{
            cur = null;
        }

        if(op == Predicate.Op.EQUALS){
            if(cur==null){
                return 0.0;
            }
            return (cur.count/cur.w)/(tupleNum*1.0);
        }else if(op == Predicate.Op.GREATER_THAN){
            if(v<min){
                return 1.0;
            }else if(v>=max){
                return 0.0;
            }else if(cur!=null){
                double res = ((cur.right-v)/cur.w)*(cur.count*1.0)/(tupleNum*1.0);
                for(int i  =target+1;i<bucketNum;i++){
                    res += (buckets[i].count *1.0)/(tupleNum*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.LESS_THAN){
            if(v<=min){
                return 0.0;
            }else if(v >max){
                return 1.0;
            }else if (cur!=null){
                double res =  ((v-cur.left)/cur.w)*(cur.count*1.0)/(tupleNum*1.0);
                for(int i=0;i<target;i++){
                    res += (buckets[i].count*1.0)/(tupleNum*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.NOT_EQUALS){
            if(cur==null){
                return 1.0;
            }
            return 1-((cur.count/cur.w)/(tupleNum*1.0));
        }else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            if(v<=min){
                return 1.0;
            }else if(v>max){
                return 0.0;
            }else if(cur!=null){
                double res = ((cur.right-v+1)/cur.w)*(cur.count*1.0)/(tupleNum*1.0);
                for(int i  =target+1;i<bucketNum;i++){
                    res += (buckets[i].count *1.0)/(tupleNum*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            if(v<min){
                return 0.0;
            }else if(v >=max){
                return 1.0;
            }else if (cur!=null){
                double res =  ((v-cur.left+1)/cur.w)*(cur.count*1.0)/(tupleNum*1.0);
                for(int i=0;i<target;i++){
                    res += (buckets[i].count*1.0)/(tupleNum*1.0);
                }
                return res;
            }
        }
        return 0.0;
    }




}
