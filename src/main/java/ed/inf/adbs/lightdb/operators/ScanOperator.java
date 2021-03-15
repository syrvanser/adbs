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

    @Override
    public Tuple getNextTuple() {
        if (scan.hasNext()) {
            String line = scan.nextLine();
            List<Long> record = Arrays.stream(line.split(",")).map(Long::valueOf).collect(Collectors.toList()); // convert csv string to List<Long>
            return new Tuple(record);
        } else {
            return null;
        }
    }

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
