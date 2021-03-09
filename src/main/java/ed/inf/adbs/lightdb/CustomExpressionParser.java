package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.LongValue;

import java.util.Stack;

public class CustomExpressionParser extends ExpressionDeParser {
    final protected Tuple tuple;
    final protected Stack<Long> stack;
    final protected DatabaseCatalog databaseCatalog;
    final protected String tableName;

    public CustomExpressionParser(Tuple tuple, String tableName){
        this.tuple = tuple;
        stack = new Stack<>();
        this.tableName = tableName;
        databaseCatalog = DatabaseCatalog.getInstance();
    }

    @Override
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(right & left);
    }

    @Override
    public void visit(Column tableColumn) {
        super.visit(tableColumn);
        int index = databaseCatalog.getColumnIndex(tableName, tableColumn.getFullyQualifiedName());
        stack.push(tuple.get(index));
    }

    @Override
    public void visit(LongValue longValue) {
        super.visit(longValue);
        stack.push(longValue.getValue());
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left != right? 1L :0);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left == right? 1L :0);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left > right? 1L :0);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left >= right? 1L :0);
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left < right? 1L :0);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left <= right? 1L :0);
    }

    public boolean getResult(){
        if (stack.size() == 1) {
            long longResult = stack.pop();
            if (longResult == 0) {
                return false;
            } else if (longResult == 1) {
                return true;
            }
        }
        throw new NullPointerException("Stack error!");
    }

}
