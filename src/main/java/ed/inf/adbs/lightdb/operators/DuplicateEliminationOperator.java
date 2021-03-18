package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.Tuple;

/**
 * This class removes duplicate tuples. Its parent should be either a SortOperator or an operator that had it tuples sorted before.
 */
public class DuplicateEliminationOperator extends Operator {

    final protected Operator child;
    private Tuple lastTuple;

    /**
     * Class constructor
     *
     * @param child child operator
     */
    public DuplicateEliminationOperator(Operator child) {
        super(child.getTableName());
        this.child = child;
        lastTuple = null;
    }

    /**
     * Returns the next tuple
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple;
        while ((nextTuple = child.getNextTuple()) != null) { // while there are tuples left
            if (!nextTuple.equals(lastTuple)) { // check if it's equal to the previous tuple
                lastTuple = nextTuple;
                return nextTuple;
            }
        }
        return null;
    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        lastTuple = null;
    }
}
