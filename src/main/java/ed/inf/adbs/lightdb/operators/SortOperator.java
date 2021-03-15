package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class SortOperator extends Operator {

    final protected Operator child;
    final protected List<Column> order;
    private boolean first;
    private final List<Tuple> buffer;

    public SortOperator(Operator child, List<Column> order) {
        super(child.getTableName());
        this.child = child;
        this.order = order;
        this.first = true;
        this.buffer = new ArrayList<>();
    }

    @Override
    public Tuple getNextTuple() {
        if (first) {
            Tuple lastTuple;
            while ((lastTuple = this.child.getNextTuple()) != null) {
                buffer.add(lastTuple);
            }
            buffer.sort(new TupleComparator(order, tableName));
            this.first = false;
        }
        if (buffer.size() > 0) {
            return buffer.remove(0);
        } else {
            return null;
        }
    }

    @Override
    public void reset() {
        this.first = true;
    }
}
