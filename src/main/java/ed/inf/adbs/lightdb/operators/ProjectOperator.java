package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.ParseException;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for applying a projection to a table
 */
public class ProjectOperator extends Operator {
    final protected List<SelectItem> columns;
    final protected Operator child;
    final protected List<String> newColumns;

    /**
     * Class constructor
     *
     * @param child   the child operator
     * @param columns a list of columns to project to
     */
    public ProjectOperator(Operator child, List<SelectItem> columns) {
        super(child.getTableName());
        this.child = child;
        this.columns = columns;
        this.newColumns = new ArrayList<>();
    }

    /**
     * Returns the next tuple
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = child.getNextTuple();
        List<Long> reducedValuesList = new ArrayList<>(); // list of values to keep
        if (tuple != null) {
            for (SelectItem columnSelectItem : columns) {
                if (columnSelectItem instanceof AllColumns) { // do nothing if AllColumns is present
                    return tuple;
                } else if (columnSelectItem instanceof SelectExpressionItem) {
                    SelectExpressionItem columnSelectExpressionItem = (SelectExpressionItem) columnSelectItem;
                    Column column = (Column) columnSelectExpressionItem.getExpression();
                    reducedValuesList.add(tuple.get(databaseCatalog.getColumnIndex(tableName, column.getFullyQualifiedName()))); // add the selected column to the list
                } else {
                    throw new ParseException("Invalid projection encountered!");
                }
            }
            return new Tuple(reducedValuesList);
        } else { // Once all tuples are returned, we need to remap columns in the column directory
            for (SelectItem columnSelectItem : columns) {
                if (columnSelectItem instanceof SelectExpressionItem) { // only do this for columns, not "AllColumns"
                    SelectExpressionItem columnSelectExpressionItem = (SelectExpressionItem) columnSelectItem;
                    Column column = (Column) columnSelectExpressionItem.getExpression();
                    newColumns.add(column.getFullyQualifiedName());
                }
            }
            if (!newColumns.isEmpty()) { // if there are columns to remove, pass the list of columns that we want to keep to the directory and remove the rest
                databaseCatalog.filter(tableName, newColumns);
            }

            return null;
        }

    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        child.reset();
    }
}
