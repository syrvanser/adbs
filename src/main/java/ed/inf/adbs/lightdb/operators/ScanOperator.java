package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class ScanOperator extends Operator {

    private Scanner scan;

    /**
     * Class constructor
     *
     * @param tableName table to scan from
     */
    public ScanOperator(String tableName) {
        super(tableName);
        File tableFile = databaseCatalog.getFile(tableName);
        try {
            scan = new Scanner(tableFile);
        } catch (FileNotFoundException e) {
            System.err.println("File for table " + tableName + " cannot be found!");
            exit(-1);
        }
    }

    /**
     * Returns the next tuple
     *
     * @return next tuple (null if no tuples are left)
     */
    @Override
    public Tuple getNextTuple() {
        if (scan.hasNext()) {
            String line = scan.nextLine();
            List<Long> record = Arrays.stream(line.split(",")).map(Long::valueOf).collect(Collectors.toList()); // converts csv string to List<Long>
            return new Tuple(record);
        } else {
            return null;
        }
    }

    /**
     * Method for resetting the operator
     */
    @Override
    public void reset() {
        File tableFile = databaseCatalog.getFile(tableName);
        try {
            scan = new Scanner(tableFile);
        } catch (FileNotFoundException e) {
            System.err.println("File for table " + tableName + " cannot be found!");
            exit(-1);
        }
    }
}
