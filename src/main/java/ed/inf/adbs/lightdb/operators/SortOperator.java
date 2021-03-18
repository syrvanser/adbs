package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * This class uses TimSort to sort the tuples using the columns prvided
 */
public class SortOperator extends Operator {

    final protected Operator child;
    final protected List<Column> order;
    final private List<Tuple> buffer;

    /**
     * Class constructor
     *
     * @param child Child operator
     * @param order Columns to sort by (starting from highest priority)
     */
    public SortOperator(Operator child, List<Column> order) {
        super(child.getTableName());
        this.child = child;
        this.order = order;
        this.buffer = new ArrayList<>();
    }

    /**
     * Returns the next tuple
     * This method has to repeatedly call "getNextTuple" on its children because all children are required for this sort to work
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        if (buffer.size() == 0) {
            Tuple lastTuple;
            while ((lastTuple = this.child.getNextTuple()) != null) {
                buffer.add(lastTuple);
            }
            buffer.sort(new TupleComparator(order, tableName)); // use a custom comparator
        }
        if (buffer.size() > 0) { // pop one element from the buffer
            return buffer.remove(0);
        } else {
            return null;
        }
    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        this.buffer.clear();
    }
}
