package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.DatabaseCatalog;
import ed.inf.adbs.lightdb.Tuple;

import java.io.PrintStream;

/**
 * An abstract base class for operators
 */
public abstract class Operator {

    final protected DatabaseCatalog databaseCatalog;
    final protected String tableName;

    /**
     * Class constructor
     *
     * @param tableName table name for the table the operator produces
     */
    public Operator(String tableName) {
        this.tableName = tableName;
        databaseCatalog = DatabaseCatalog.getInstance();
    }

    /**
     * Table name getter
     *
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Abstract method for generating a single tuple
     *
     * @return the tuple
     */
    public abstract Tuple getNextTuple();

    /**
     * Abstract method for resetting the operator
     */
    public abstract void reset();

    /**
     * Dumps all tuples using the PrintStream provided
     *
     * @param p PrintStream to dump to
     */
    public void dump(PrintStream p) {
        Tuple tuple = getNextTuple();
        while (tuple != null) {
            p.println(tuple);
            tuple = getNextTuple();
        }
    }
}
