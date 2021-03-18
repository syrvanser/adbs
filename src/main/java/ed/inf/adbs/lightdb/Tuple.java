package ed.inf.adbs.lightdb;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class for storing a single tuple of Long numbers
 */
public class Tuple {
    private final List<Long> data;

    /**
     * Constructor
     *
     * @param data A list of Longs to create the tuple from
     */
    public Tuple(List<Long> data) {
        this.data = data;
    }

    /**
     * A method for getting a value using an index
     *
     * @param index element position (starts at 0)
     * @return element value
     */
    public long get(int index) {
        return data.get(index);
    }

    /**
     * Method for getting tuple size
     *
     * @return tuple size
     */
    public int size() {
        return data.size();
    }

    /**
     * Returns a list representation of the tuple
     *
     * @return List representation
     */
    public List<Long> toList() {
        return new ArrayList<>(data); // to ensure the tuple isn't modified
    }

    /**
     * Method for getting a String representation of the tuple
     *
     * @return String representation of the tuple
     */
    @Override
    public String toString() {
        return data.stream().map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    /**
     * Method that allows to check if two tuples have the same values at each position
     *
     * @param o the other tuple
     * @return true if they are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Tuple)) {
            return false;
        }

        Tuple t = (Tuple) o;

        if (t.toList().size() != this.toList().size()) {
            return false;
        }

        for (int i = 0; i < t.toList().size(); i++) {
            if (this.get(i) != t.get(i)) {
                return false;
            }
        }
        return true;
    }

}
