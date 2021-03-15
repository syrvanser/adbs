package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.operators.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryPlanExecutor {
    private static QueryPlanExecutor INSTANCE;
    final protected DatabaseCatalog databaseCatalog = DatabaseCatalog.getInstance();

    private QueryPlanExecutor() {

    }

    public static QueryPlanExecutor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new QueryPlanExecutor();
        }
        return INSTANCE;
    }

    public void reset() {
        INSTANCE = new QueryPlanExecutor();
    }

    private String getTableName(Table table) {
        if (table.getAlias() != null) {
            return table.getAlias().getName();
        } else {
            return table.getName();
        }
    }

    public Operator executeStatement(Select select) {

        Operator root;
        PlainSelect plainSelectBody = (PlainSelect) select.getSelectBody();

        String leftTableName = getTableName(((Table) plainSelectBody.getFromItem()));
        if (plainSelectBody.getFromItem().getAlias() != null) {
            databaseCatalog.addAlias(leftTableName, ((Table) plainSelectBody.getFromItem()).getName());
        }

        if (plainSelectBody.getJoins() == null) {
            root = new ScanOperator(leftTableName);
            if (plainSelectBody.getWhere() != null) {
                root = new SelectOperator(root, plainSelectBody.getWhere());
            }
        } else {
            List<String> items = plainSelectBody.getJoins().stream().map(Join::getRightItem).map(t -> getTableName((Table) t)).collect(Collectors.toList());
            for (int i = 0; i < plainSelectBody.getJoins().size(); i++) {
                databaseCatalog.addAlias(getTableName((Table) plainSelectBody.getJoins().get(i).getRightItem()), ((Table) plainSelectBody.getJoins().get(i).getRightItem()).getName());
            }

            if (plainSelectBody.getWhere() == null) {
                root = new ScanOperator(leftTableName);
                for (Join join : plainSelectBody.getJoins()) {
                    root = new JoinOperator(root, new ScanOperator(getTableName((Table) join.getRightItem())), null);
                }
            } else {
                items.add(0, leftTableName);
                CustomJoinExpressionParser customJoinExpressionParser = new CustomJoinExpressionParser(items);
                plainSelectBody.getWhere().accept(customJoinExpressionParser);
                root = customJoinExpressionParser.buildTree();
            }
        }
        root = new ProjectOperator(root, plainSelectBody.getSelectItems());
        if (plainSelectBody.getOrderByElements() != null) {
            root = new SortOperator(root, plainSelectBody.getOrderByElements().stream().map(OrderByElement::getExpression).map(t -> ((Column) t)).collect(Collectors.toList()));
        }

        if (plainSelectBody.getDistinct() != null) {
            if (plainSelectBody.getOrderByElements() == null) {
                root = new SortOperator(root, null);
            }
            root = new DuplicateEliminationOperator(root);
        }
        return root;
    }
}

