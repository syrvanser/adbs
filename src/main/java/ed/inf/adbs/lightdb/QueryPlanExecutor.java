package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.operators.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is responsible for creating and executing the query plan
 **/
public final class QueryPlanExecutor {
    private static QueryPlanExecutor INSTANCE;
    final private DatabaseCatalog databaseCatalog = DatabaseCatalog.getInstance();

    // Empty private constructor
    private QueryPlanExecutor() {
    }

    /**
     * A method for getting the QueryPlanExecutor instance
     *
     * @return QueryPlanExecutor instance
     */
    public static QueryPlanExecutor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new QueryPlanExecutor();
        }
        return INSTANCE;
    }

    /**
     * Method for resetting the instance
     */
    public void reset() {
        INSTANCE = new QueryPlanExecutor();
    }

    /**
     * A utility method for looking up the real table name
     *
     * @param table Table instance
     * @return String table name (ignoring the alias)
     */
    private String getTableName(Table table) {
        if (table.getAlias() != null) {
            return table.getAlias().getName();
        } else {
            return table.getName();
        }
    }

    /**
     * The base method for executing a select statement
     *
     * @param select Statement to execute
     * @return The root operator of the query plan tree
     */
    public Operator executeStatement(Select select) {

        Operator root;
        PlainSelect plainSelectBody = (PlainSelect) select.getSelectBody();

        String leftTableName = getTableName(((Table) plainSelectBody.getFromItem())); // the name of the first table in the "From" part
        if (plainSelectBody.getFromItem().getAlias() != null) { // if an alias is present
            databaseCatalog.addAlias(leftTableName, ((Table) plainSelectBody.getFromItem()).getName()); // add the table alias to the alias directory
        }

        if (plainSelectBody.getJoins() == null) { // if there are no joins, simply use a scan operator and a where operator
            root = new ScanOperator(leftTableName);
            if (plainSelectBody.getWhere() != null) {
                root = new SelectOperator(root, plainSelectBody.getWhere());
            }
        } else { // if there is a join, we need to process them
            List<String> items = plainSelectBody.getJoins().stream().map(Join::getRightItem).map(t -> getTableName((Table) t)).collect(Collectors.toList()); // convert the joins to a list of String table names
            for (int i = 0; i < plainSelectBody.getJoins().size(); i++) {
                databaseCatalog.addAlias(items.get(i), ((Table) plainSelectBody.getJoins().get(i).getRightItem()).getName()); // add each alias to the alias directory
            }

            if (plainSelectBody.getWhere() == null) { // if a where clause is present, create join operators (and a scan operator for each table)
                root = new ScanOperator(leftTableName);
                for (Join join : plainSelectBody.getJoins()) {
                    root = new JoinOperator(root, new ScanOperator(getTableName((Table) join.getRightItem())), null); // just chain them as a left tree
                }
            } else { // if a where clause is present, we can't just chain them together
                items.add(0, leftTableName); // add the first table at the beginning
                CustomJoinExpressionParser customJoinExpressionParser = new CustomJoinExpressionParser(items); // create a parser for the where clause
                plainSelectBody.getWhere().accept(customJoinExpressionParser); // visit the select body
                root = customJoinExpressionParser.buildTree(); //build the tree and save the root as the current root
            }
        }

        root = new ProjectOperator(root, plainSelectBody.getSelectItems()); // apply projection
        if (plainSelectBody.getOrderByElements() != null) { // add order by operator (if needed)
            root = new SortOperator(root, plainSelectBody.getOrderByElements().stream().map(OrderByElement::getExpression).map(t -> ((Column) t)).collect(Collectors.toList()));  // convert all order by elements to columns
        }

        if (plainSelectBody.getDistinct() != null) { // add distinct operator (if needed)
            if (plainSelectBody.getOrderByElements() == null) {
                root = new SortOperator(root, null); // sort if there is no group by operator
            }
            root = new DuplicateEliminationOperator(root); // make the distinct operator the new root
        }
        return root;
    }
}

