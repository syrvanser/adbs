package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.operators.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.LongValue;


import java.util.*;

import static java.lang.Math.max;

public class CustomJoinExpressionParser extends ExpressionDeParser {
    final protected Expression[] joinExpressions;
    final protected Expression[] selectExpressions;
    final protected List<String> items;
    final protected Map<String, Integer> indexMapping = new HashMap<>();

    public CustomJoinExpressionParser(List<String> items){
        this.items = items;
        for (int i = 0; i < items.size(); i++){
            indexMapping.put(items.get(i), i);
        }
        selectExpressions = new Expression[items.size()];
        joinExpressions = new Expression[items.size()];
    }

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

    protected void parseBinaryExpression(BinaryExpression expression){
        Expression left = expression.getLeftExpression();
        Expression right = expression.getRightExpression();
        if (left instanceof Column && right instanceof Column && !((Column) left).getTable().getName().equals(((Column) right).getTable().getName())){
//            System.out.println(((Column) left).getTable().getName());
//            System.out.println(((Column) right).getTable().getName());
//            System.out.println(indexMapping);
            int mergeIndex = max(indexMapping.get(((Column) left).getTable().getName()), indexMapping.get(((Column) right).getTable().getName()));
            if (joinExpressions[mergeIndex] == null){
                joinExpressions[mergeIndex] = expression;
            } else {
                joinExpressions[mergeIndex] = new AndExpression(joinExpressions[mergeIndex], expression);
            }
        } else {
            if (left instanceof Column){
                int mergeIndex = indexMapping.get(((Column) left).getTable().getName());
                if (selectExpressions[mergeIndex] == null){
                    selectExpressions[mergeIndex] = expression;
                } else {
                    selectExpressions[mergeIndex] = new AndExpression(selectExpressions[mergeIndex], expression);
                }
            } else if (right instanceof Column) {
                int mergeIndex = indexMapping.get(((Column) right).getTable().getName());
                if (selectExpressions[mergeIndex] == null){
                    selectExpressions[mergeIndex] = expression;
                } else {
                    selectExpressions[mergeIndex] = new AndExpression(selectExpressions[mergeIndex], expression);
                }
            }
            else {
                throw new ParseException("Parse error encountered during join parsing!");
            }
        }
    }

    public Operator buildTree(){
        Operator root = new ScanOperator(items.get(0));
//        System.out.println(items);
//        System.out.println(Arrays.toString(selectExpressions));
//        System.out.println(Arrays.toString(joinExpressions));
        if (selectExpressions[0] != null){
            root = new SelectOperator(root, selectExpressions[0]);
        }
        for (int i = 1; i < items.size(); i++){
            Operator rightChild = new ScanOperator(items.get(i));
            if (selectExpressions[i] != null)
                rightChild = new SelectOperator(rightChild, selectExpressions[i]);

            root = new JoinOperator(root, rightChild, joinExpressions[i]);
        }
        return root;
    }

}
