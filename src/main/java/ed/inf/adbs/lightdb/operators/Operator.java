package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.DatabaseCatalog;
import ed.inf.adbs.lightdb.Tuple;

import java.io.PrintStream;

public abstract class Operator {

    final protected DatabaseCatalog databaseCatalog;
    final protected String tableName;

    public Operator(String tableName){
        this.tableName = tableName;
        databaseCatalog = DatabaseCatalog.getInstance();
    }

    public String getTableName(){
        return tableName;
    }

    public abstract Tuple getNextTuple();

    public abstract void reset();

    public void dump(PrintStream p){
        Tuple tuple = getNextTuple();
        while (tuple != null) {
            p.println(tuple);
            tuple = getNextTuple();
        }
    }
}
