package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.ParseException;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {
    final protected List<SelectItem> columns;
    final protected Operator child;
    final private List<String> newColumns;

    public ProjectOperator(Operator child, List<SelectItem> columns) {
        super(child.getTableName());
        this.child = child;
        this.columns = columns;
        this.newColumns = new ArrayList<>();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = child.getNextTuple();
        List<Long> reducedValuesList = new ArrayList<>();
        if (tuple != null) {
            for (SelectItem columnSelectItem : columns) {
                if (columnSelectItem instanceof AllColumns) {
                    return tuple;
                } else if (columnSelectItem instanceof SelectExpressionItem) {
                    SelectExpressionItem columnSelectExpressionItem = (SelectExpressionItem) columnSelectItem;
                    Column column = (Column) columnSelectExpressionItem.getExpression();
                    reducedValuesList.add(tuple.get(databaseCatalog.getColumnIndex(tableName, column.getFullyQualifiedName())));
                } else {
                    throw new ParseException("Invalid projection encountered!");
                }
            }
            return new Tuple(reducedValuesList);
        } else {
            for (SelectItem columnSelectItem : columns) {
                if (columnSelectItem instanceof SelectExpressionItem) {
                    SelectExpressionItem columnSelectExpressionItem = (SelectExpressionItem) columnSelectItem;
                    Column column = (Column) columnSelectExpressionItem.getExpression();
                    newColumns.add(column.getFullyQualifiedName());
                }

            }
            if (!newColumns.isEmpty()) {
                databaseCatalog.filter(tableName, newColumns);
            }

            return null;
        }

    }

    @Override
    public void reset() {
        child.reset();
    }
}
