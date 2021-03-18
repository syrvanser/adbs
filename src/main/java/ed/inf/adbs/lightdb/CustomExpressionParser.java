package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.Stack;

/**
 * A class that parses a WHERE clause
 */
public class CustomExpressionParser extends ExpressionDeParser {
    final protected Tuple tuple;
    final protected Stack<Long> stack;
    final protected DatabaseCatalog databaseCatalog;
    final protected String tableName;

    /**
     * Class constructor
     *
     * @param tuple     Tuple to check
     * @param tableName Tuple's table
     */
    public CustomExpressionParser(Tuple tuple, String tableName) {
        this.tuple = tuple;
        stack = new Stack<>(); // we use a stack of numbers to store intermediate results. For binary results, "0" is used to store "False" and "1" is used to store "True"
        this.tableName = tableName;
        databaseCatalog = DatabaseCatalog.getInstance();
    }

    /**
     * The following methods are all overridden methods from the parent class that perform parsing
     */

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
        stack.push(left != right ? 1L : 0);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left == right ? 1L : 0);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left > right ? 1L : 0);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left >= right ? 1L : 0);
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left < right ? 1L : 0);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        long right = stack.pop();
        long left = stack.pop();
        stack.push(left <= right ? 1L : 0);
    }

    /**
     * This function should be called after the expression is parsed. It checks that a single value is left on the stack, and returns
     *
     * @return 1 if the tuple is accepted, 0 if it's rejected
     */
    public boolean getResult() {
        if (stack.size() == 1) {
            long longResult = stack.pop();
            return longResult != 0;
        }
        throw new NullPointerException("Stack error!");
    }

}
