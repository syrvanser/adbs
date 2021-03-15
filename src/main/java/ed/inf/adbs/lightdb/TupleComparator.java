package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TupleComparator implements Comparator<Tuple> {

    protected final DatabaseCatalog databaseCatalog;
    protected final List<Column> attributes;
    protected final String table;

    public TupleComparator(List<Column> attributes, String table) {
        this.attributes = attributes;
        this.table = table;
        this.databaseCatalog = DatabaseCatalog.getInstance();
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        List<Integer> visitedIndexes = new ArrayList<>();
        if (attributes != null) {
            for (Column attribute : attributes) {
                int index = databaseCatalog.getColumnIndex(table, attribute.getFullyQualifiedName()); //rewrite to handle alisases?
                visitedIndexes.add(index);
                if (t1.get(index) > t2.get(index)) {
                    return 1;
                } else if (t1.get(index) < t2.get(index)) {
                    return -1;
                }
            }
        }

        for (int index = 0; index < t1.size(); index++) {
            if (!visitedIndexes.contains(index)) {
                if (t1.get(index) > t2.get(index)) {
                    return 1;
                } else if (t1.get(index) < t2.get(index)) {
                    return -1;
                }
            }
        }

        return 0;
    }
}
