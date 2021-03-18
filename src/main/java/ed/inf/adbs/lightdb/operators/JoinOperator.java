package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.CustomExpressionParser;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An operator that performs a simple nested join for two tables
 */
public class JoinOperator extends Operator {
    final protected Operator leftChild, rightChild;
    final protected Expression expression;
    protected Tuple currentLeftTuple;

    /**
     * Class constructor
     *
     * @param leftChild  left child
     * @param rightChild right child
     * @param expression WHERE expression (can be null)
     */
    public JoinOperator(Operator leftChild, Operator rightChild, Expression expression) {
        super(leftChild.getTableName() + "_join_" + rightChild.getTableName());
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.expression = expression;
        databaseCatalog.addTableJoin(leftChild.getTableName(), rightChild.getTableName());
        currentLeftTuple = null;
    }

    /**
     * Returns the next tuple
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        if (currentLeftTuple == null) { // get the left tuple if this is the first time we run this
            currentLeftTuple = leftChild.getNextTuple();
        }
        Tuple currentRightTuple;
        while (currentLeftTuple != null) { // keep running until we run out of left tuples
            while ((currentRightTuple = rightChild.getNextTuple()) != null) { // get the right tuple
                Tuple mergedTuple = new Tuple(Stream.concat(currentLeftTuple.toList().stream(), currentRightTuple.toList().stream())
                        .collect(Collectors.toList())); // make a new tuple
                CustomExpressionParser parser = new CustomExpressionParser(mergedTuple, getTableName()); // make a new parser for the WHERE clause
                if (expression == null) { // if the expression is not present, just return the tuple
                    return mergedTuple;
                }
                expression.accept(parser); // check if the tuple satisfies the expression

                if (parser.getResult()) {
                    return mergedTuple; // return it if it does, otherwise carry on getting tuples from the right
                }
            }
            rightChild.reset(); // reset the right child if we run out of tuples on the right
            currentLeftTuple = leftChild.getNextTuple(); // get a new left tuple
        }
        return null;
    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
