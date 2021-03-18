package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.operators.JoinOperator;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

/**
 * This class parses the where clause in a Join expression and creates a query plan based on it
 */
public class CustomJoinExpressionParser extends ExpressionDeParser {
    final private Expression[] joinExpressions; // a list of "join" expressions (which have columns from different tables on two sides, e. g. "Sailors.A > Boats.B"
    final private Expression[] selectExpressions; // a list of regular expressions (with one table or less, like "1=1" or "Sailors.A != 42)
    final private List<String> items;
    final private Map<String, Integer> indexMapping = new HashMap<>();

    /**
     * Class constructor
     *
     * @param items a list of table names in the SELECT part of the expression
     */
    public CustomJoinExpressionParser(List<String> items) {
        this.items = items;
        for (int i = 0; i < items.size(); i++) {
            indexMapping.put(items.get(i), i);
        }
        selectExpressions = new Expression[items.size()];
        joinExpressions = new Expression[items.size()];
    }

    /**
     * Following methods parse the WHERE clause. AND, Column and LongValue behave like their parent and are overridden here for readability only.
     */

    @Override
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
    }

    @Override
    public void visit(Column tableColumn) {
        super.visit(tableColumn);
    }

    @Override
    public void visit(LongValue longValue) {
        super.visit(longValue);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        parseBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        parseBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        parseBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        parseBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        parseBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        parseBinaryExpression(minorThanEquals);
    }

    /**
     * This method parses a binary expression (relational operators only)
     *
     * @param expression the expression to parse
     */
    protected void parseBinaryExpression(BinaryExpression expression) {
        Expression left = expression.getLeftExpression(); // left side of the expression
        Expression right = expression.getRightExpression(); // right side
        // if each side is a column, and they are from different tables
        if (left instanceof Column && right instanceof Column && !((Column) left).getTable().getName().equals(((Column) right).getTable().getName())) {
            // we determine which column belongs to a table that is later (= higher) in the merge tree
            // for example, if we join A, B, C, then mergeIndex(A,C) = 2 (because we start from 0)
            int mergeIndex = max(indexMapping.get(((Column) left).getTable().getName()), indexMapping.get(((Column) right).getTable().getName()));
            if (joinExpressions[mergeIndex] == null) { // if the map doesn't have any value, we simply put the expression there
                joinExpressions[mergeIndex] = expression;
            } else {
                // if there is a value for this index, we append the new expression using and AND operator
                joinExpressions[mergeIndex] = new AndExpression(joinExpressions[mergeIndex], expression);
            }
        } else { // if only one side or less is a column (or they are from the same table)
            if (left instanceof Column) { // if the left side is a column (this covers both "Sailors.A > 0" and "Sailors.A > Sailors.B" type expressions)
                int mergeIndex = indexMapping.get(((Column) left).getTable().getName()); // this column's index (i. e. its position in the items list)
                if (selectExpressions[mergeIndex] == null) { // if there are no other expressions there, put it there
                    selectExpressions[mergeIndex] = expression;
                } else {
                    selectExpressions[mergeIndex] = new AndExpression(selectExpressions[mergeIndex], expression); // otherwise, join using an AND
                }
            } else if (right instanceof Column) { // if the right side is a column instead (e. g. 42 >= Sailors.C)
                int mergeIndex = indexMapping.get(((Column) right).getTable().getName()); // get the index
                if (selectExpressions[mergeIndex] == null) {
                    selectExpressions[mergeIndex] = expression; // add the expression
                } else {
                    selectExpressions[mergeIndex] = new AndExpression(selectExpressions[mergeIndex], expression); // join it with AND
                }
            } else if (left instanceof LongValue && right instanceof LongValue) { // if it's something like "1 != 0" we put it at index 0 (because the earlier we evaluate it, the better)
                int mergeIndex = 0;
                if (selectExpressions[mergeIndex] == null) {
                    selectExpressions[mergeIndex] = expression; // add he expression
                } else {
                    selectExpressions[mergeIndex] = new AndExpression(selectExpressions[mergeIndex], expression); // join with AND
                }
            } else {
                throw new ParseException("Parse error encountered during join parsing!");
            }
        }
    }

    /**
     * This method is called after parsing is complete. It iterates over both lists and create a query plan
     *
     * @return the root operator of the tree
     */
    public Operator buildTree() {
        Operator root = new ScanOperator(items.get(0)); // scan operator for the leftmost child
        if (selectExpressions[0] != null) { // if there are select expressions for it, make a select operator
            root = new SelectOperator(root, selectExpressions[0]);
        }
        for (int i = 1; i < items.size(); i++) { // go over all other join items
            Operator rightChild = new ScanOperator(items.get(i)); // scan operator for the right child
            if (selectExpressions[i] != null) // if it has any select expressions, add a select operator to it BEFORE the join
                rightChild = new SelectOperator(rightChild, selectExpressions[i]);

            root = new JoinOperator(root, rightChild, joinExpressions[i]); // merge the root with the new child
        }
        return root;
    }
}
