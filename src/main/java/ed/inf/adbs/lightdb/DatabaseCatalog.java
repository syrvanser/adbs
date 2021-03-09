package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
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

    public void loadSchema(String dir){
        try {
            for (Scanner sc = new Scanner(new File(dir + File.separator + "schema.txt")); sc.hasNext(); ) {
                String[] line = sc.nextLine().split(" ");
                String tableName = line[0];
                fileCatalog.put(tableName, new File(dir + File.separator + "data" + File.separator + tableName + ".csv"));
                Map<String, Integer> columnMap = new HashMap<>(); //maintains insertion, allows bidirectional lookup
                for (int i = 1; i < line.length; i++){
                    columnMap.put( tableName + "." + line[i], i-1);
                }
                schemaCatalog.put(tableName, columnMap);
                aliasCatalog.put(tableName, tableName);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void addTableJoin(String table1, String table2){
        Map<String, Integer> columnMap = new HashMap<>();
        String joinName = table1+"_join_"+table2;
        for (String column : schemaCatalog.get(table1).keySet()){
            columnMap.put(column, schemaCatalog.get(table1).get(column));
        }
        int offset = columnMap.size();
        for (String column : schemaCatalog.get(table2).keySet()){
            columnMap.put(column, schemaCatalog.get(table2).get(column) + offset);
        }
        schemaCatalog.put(joinName, columnMap);
        aliasCatalog.put(joinName, joinName);
    }

    public void addAlias(String alias, String realName){
        aliasCatalog.put(alias, realName);
        Map<String, Integer> columnMap = new HashMap<>();
        for (String column : schemaCatalog.get(realName).keySet()){
            columnMap.put(column.replaceFirst(realName, alias), schemaCatalog.get(realName).get(column));
        }
        schemaCatalog.put(alias, columnMap);

    }

    public String getRealName(String alias){
        return aliasCatalog.get(alias);
    }

    public static DatabaseCatalog getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DatabaseCatalog();
        }
        return INSTANCE;
    }

    public File getFile(String table){
        table = aliasCatalog.get(table);
        return fileCatalog.get(table);
    }

    public int getColumnIndex(String table, String name){
//        System.out.println(table);
//        System.out.println(name);
//        System.out.println(schemaCatalog);
        //table = aliasCatalog.get(table);
        return schemaCatalog.get(table).get(name);
    }

}