package optimizer;

import execution.Predicate;

public class LogicalFilterNode {

    public final String table;//表名

    public final Predicate.Op operator;//操作符

    public final String constant;//常数

    public final String fieldPureName;//属性名

    public final String fieldQuantifiedName;//属性全名（表名.属性名）

    public LogicalFilterNode(String table, String field, Predicate.Op operator, String constant) {
        this.table = table;
        this.operator=operator;
        this.constant = constant;
        String[] tmps = field.split("[.]");
        if (tmps.length>1)
            fieldPureName = tmps[tmps.length-1];
        else
            fieldPureName=field;
        this.fieldQuantifiedName = table+"."+fieldPureName;
    }
}
