package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.CustomExpressionParser;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {
    final protected Expression expression;
    final protected Operator child;

    public SelectOperator(Operator child, Expression expression) {
        super(child.getTableName());
        this.child = child;
        this.expression = expression;
    }

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

    @Override
    public void reset() {
        child.reset();
    }
}
