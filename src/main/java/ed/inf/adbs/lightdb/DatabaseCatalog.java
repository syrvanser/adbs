package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.schema.Column;

import javax.sound.midi.Soundbank;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLOutput;
import java.util.*;

public final class DatabaseCatalog {
    private static DatabaseCatalog INSTANCE;
    private final Map<String, File> fileCatalog;
    private final Map<String, Map<String, Integer>> schemaCatalog;
    private final Map<String, String> aliasCatalog;

    private DatabaseCatalog() {
        fileCatalog = new HashMap<>();
        schemaCatalog = new HashMap<>();
        aliasCatalog = new HashMap<>();
    }

    public static DatabaseCatalog getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DatabaseCatalog();
        }
        return INSTANCE;
    }

    public void reset() {
        INSTANCE = new DatabaseCatalog();
    }

    public void loadSchema(String dir) {
        try {
            for (Scanner sc = new Scanner(new File(dir + File.separator + "schema.txt")); sc.hasNext(); ) {
                String[] line = sc.nextLine().split(" ");
                String tableName = line[0];
                fileCatalog.put(tableName, new File(dir + File.separator + "data" + File.separator + tableName + ".csv"));
                Map<String, Integer> columnMap = new HashMap<>(); //maintains insertion, allows bidirectional lookup
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

    public void addTableJoin(String table1, String table2) {
        Map<String, Integer> columnMap = new HashMap<>();
        String joinName = table1 + "_join_" + table2;
        String originalTable1 = getRealName(table1);
        String originalTable2 = getRealName(table2);
//        System.out.println(table1);
//        System.out.println(table2);
//        System.out.println(schemaCatalog);
        for (String column : schemaCatalog.get(originalTable1).keySet()) {
//            System.out.println(column);
//            System.out.println(schemaCatalog);
            if (!originalTable1.equals(table1)) { // if alias is used
                String aliasedColumn = column.replaceFirst(column.split("\\.")[0], table1); //get real table name
                columnMap.put(aliasedColumn, schemaCatalog.get(originalTable1).get(column)); //look up in the real table
            } else {
                columnMap.put(column, schemaCatalog.get(originalTable1).get(column));

            }
        }
        int offset = columnMap.size();
        for (String column : schemaCatalog.get(originalTable2).keySet()) {
            if (!originalTable2.equals(table2)) {
                String aliasedColumn = column.replaceFirst(column.split("\\.")[0], table2);
                columnMap.put(aliasedColumn, schemaCatalog.get(originalTable2).get(column) + offset);
            } else {
                columnMap.put(column, schemaCatalog.get(originalTable2).get(column) + offset);
            }
        }
        schemaCatalog.put(joinName, columnMap);
        aliasCatalog.put(joinName, joinName);
    }

    public void addAlias(String alias, String realName) {
        aliasCatalog.put(alias, realName);
//        Map<String, Integer> columnMap = new HashMap<>();
//        for (String column : schemaCatalog.get(realName).keySet()){
//            columnMap.put(column.replaceFirst(realName, alias), schemaCatalog.get(realName).get(column));
//        }
//        schemaCatalog.put(alias, columnMap);

    }

    public String getRealName(String alias) {
        return aliasCatalog.get(alias);
    }

    public File getFile(String table) {
        table = aliasCatalog.get(table);
        return fileCatalog.get(table);
    }

    public int getColumnIndex(String table, String name) {
        String originalTable = aliasCatalog.get(table);

        if (table.equals(originalTable)) {  // check if it's an alias
            return schemaCatalog.get(originalTable).get(name);
        } else {
            String prefix = name.split("\\.")[0];
            String newName = name.replaceFirst(prefix, getRealName(prefix)); // if it's an alias, replace the prefix with og table name
            return schemaCatalog.get(originalTable).get(newName);
        }
    }

    public void filter(String tableName, List<String> columns) {
        HashMap<String, Integer> columnMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnMap.put(columns.get(i), i);
        }
        schemaCatalog.put(tableName, columnMap);
    }

}