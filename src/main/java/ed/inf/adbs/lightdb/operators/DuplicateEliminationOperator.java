package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.Tuple;

public class DuplicateEliminationOperator extends Operator {

    private Tuple lastTuple;
    protected Operator child;

    public DuplicateEliminationOperator(Operator child) {
        super(child.getTableName());
        this.child = child;
        lastTuple = null;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple;
        while ((nextTuple = child.getNextTuple()) != null) {
            if (!nextTuple.equals(lastTuple)) {
                lastTuple = nextTuple;
                return nextTuple;
            }
        }
        return null;
    }

    @Override
    public void reset() {
        lastTuple = null;
    }
}
