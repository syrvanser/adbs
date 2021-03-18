package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Class that allows to compare tuples based on given list of parameters
 */
public class TupleComparator implements Comparator<Tuple> {

    protected final DatabaseCatalog databaseCatalog;
    protected final List<Column> attributes;
    protected final String table;

    /**
     * Class constructor
     *
     * @param attributes list of Column attributes (in order of decreasing priority)
     * @param table      current table name
     */
    public TupleComparator(List<Column> attributes, String table) {
        this.attributes = attributes;
        this.table = table;
        this.databaseCatalog = DatabaseCatalog.getInstance();
    }

    /**
     * Method for comparing two tuples
     *
     * @param t1 first tuple
     * @param t2 second tuple
     * @return 1 if t1 is bigger, -1 if t1 is smaller, 0 if they are equal
     */
    @Override
    public int compare(Tuple t1, Tuple t2) {
        List<Integer> visitedIndexes = new ArrayList<>();
        if (attributes != null) { // if there are attributes to compare on
            for (Column attribute : attributes) { // go over them
                int index = databaseCatalog.getColumnIndex(table, attribute.getFullyQualifiedName()); // get current index
                visitedIndexes.add(index);
                if (t1.get(index) > t2.get(index)) {
                    return 1;
                } else if (t1.get(index) < t2.get(index)) {
                    return -1;
                }
            }
        }

        // if all given attributes are the same, sort on the remaining attributes starting from the leftmost unvisited attribute
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
