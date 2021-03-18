package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.CustomExpressionParser;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * Class for filtering out tuples based on a condition
 */
public class SelectOperator extends Operator {
    final protected Expression expression;
    final protected Operator child;

    /**
     * Class constructor
     *
     * @param child      child operator
     * @param expression the "where" expression
     */
    public SelectOperator(Operator child, Expression expression) {
        super(child.getTableName());
        this.child = child;
        this.expression = expression;
    }

    /**
     * Returns the next tuple
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            CustomExpressionParser parser = new CustomExpressionParser(tuple, child.getTableName());
            expression.accept(parser);
            if (parser.getResult()) {
                return tuple;
            }
        }

        return null;
    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        child.reset();
    }
}
