package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * A singleton class that stores aliases, column name-to-index mappings and file names
 */
public final class DatabaseCatalog {
    private static DatabaseCatalog INSTANCE;
    private final Map<String, File> fileCatalog;
    private final Map<String, Map<String, Integer>> schemaCatalog;
    private final Map<String, String> aliasCatalog;

    /**
     * Class constructor
     */
    private DatabaseCatalog() {
        fileCatalog = new HashMap<>();
        schemaCatalog = new HashMap<>();
        aliasCatalog = new HashMap<>();
    }

    /**
     * Method for retrieving an instance of the class
     *
     * @return class instance
     */
    public static DatabaseCatalog getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DatabaseCatalog();
        }
        return INSTANCE;
    }

    /**
     * Method for resetting the catalog
     */
    public void reset() {
        INSTANCE = new DatabaseCatalog();
    }

    /**
     * This method loads a schema file from the given directory and stores file references, column mappings and name->name alias
     *
     * @param dir schema location
     */
    public void loadSchema(String dir) {
        try {
            for (Scanner sc = new Scanner(new File(dir + File.separator + "schema.txt")); sc.hasNext(); ) {
                String[] line = sc.nextLine().split(" ");
                String tableName = line[0];
                fileCatalog.put(tableName, new File(dir + File.separator + "data" + File.separator + tableName + ".csv"));
                Map<String, Integer> columnMap = new HashMap<>(); // for each table, there is a hashmap with column names
                for (int i = 1; i < line.length; i++) {
                    columnMap.put(tableName + "." + line[i], i - 1);
                }
                schemaCatalog.put(tableName, columnMap);
                aliasCatalog.put(tableName, tableName);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method creates a new "virtual" table for each join. This is needed in order to store column name mapping in the joined table.
     *
     * @param table1 left table
     * @param table2 right table
     */
    public void addTableJoin(String table1, String table2) {
        Map<String, Integer> columnMap = new HashMap<>();
        String joinName = table1 + "_join_" + table2;

        /* We simply insert "_join_" between table names.
        This is done for readability and makes table name like "a_join_b" invalid.
        If needed, this can be replaced with a GUID or a name that contains special characters to allow such names. */

        String originalTable1 = getRealName(table1); // "unalias" the tables
        String originalTable2 = getRealName(table2);

        for (String column : schemaCatalog.get(originalTable1).keySet()) {
            if (!originalTable1.equals(table1)) { // if alias is used
                String aliasedColumn = column.replaceFirst(column.split("\\.")[0], table1); // get real table name
                columnMap.put(aliasedColumn, schemaCatalog.get(originalTable1).get(column)); // look up in the real table and put that in the map
            } else {
                columnMap.put(column, schemaCatalog.get(originalTable1).get(column)); // otherwise, just add the column

            }
        }

        // repeat for the second table
        int offset = columnMap.size();
        for (String column : schemaCatalog.get(originalTable2).keySet()) {
            if (!originalTable2.equals(table2)) {
                String aliasedColumn = column.replaceFirst(column.split("\\.")[0], table2);
                columnMap.put(aliasedColumn, schemaCatalog.get(originalTable2).get(column) + offset);
            } else {
                columnMap.put(column, schemaCatalog.get(originalTable2).get(column) + offset);
            }
        }

        // add the new joined table to the catalog
        schemaCatalog.put(joinName, columnMap);
        aliasCatalog.put(joinName, joinName);
    }

    /**
     * A method for adding an alias for a table to the catalog
     *
     * @param alias    the alias
     * @param realName the real name
     */
    public void addAlias(String alias, String realName) {
        aliasCatalog.put(alias, realName);
    }

    /**
     * A method for getting the real name of an aliased column
     *
     * @param alias the Alias
     * @return the real name
     */
    public String getRealName(String alias) {
        return aliasCatalog.get(alias);
    }

    /**
     * This method returns a File object for the table
     *
     * @param table Table name (with or without an alias)
     * @return File object
     */
    public File getFile(String table) {
        table = aliasCatalog.get(table);
        return fileCatalog.get(table);
    }

    /**
     * This method returns the column index for a column in a table
     *
     * @param table Table name (wtih or without an alias)
     * @param name  Column name
     * @return column index (starting from 0)
     */
    public int getColumnIndex(String table, String name) {
        String originalTable = aliasCatalog.get(table);

        if (table.equals(originalTable)) {  // check if it's an alias
            return schemaCatalog.get(originalTable).get(name);
        } else {
            String prefix = name.split("\\.")[0];
            String newName = name.replaceFirst(prefix, getRealName(prefix)); // if it's an alias, replace the prefix with original table name
            return schemaCatalog.get(originalTable).get(newName);
        }
    }

    /**
     * This method takes a string list of columns and removes all other columns for that table from the directory
     *
     * @param tableName table to filter
     * @param columns   list of columns to keep
     */
    public void filter(String tableName, List<String> columns) {
        HashMap<String, Integer> columnMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnMap.put(columns.get(i), i);
        }
        schemaCatalog.put(tableName, columnMap);
    }

}