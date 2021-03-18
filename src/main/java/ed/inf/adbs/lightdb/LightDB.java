package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileReader;
import java.io.PrintStream;

/**
 * Lightweight in-memory database system
 */
public class LightDB {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.err.println("Usage: LightDB database_dir input_file output_file");
            return;
        }

        String databaseDir = args[0];
        String inputFile = args[1];
        String outputFile = args[2];
        DatabaseCatalog.getInstance().loadSchema(databaseDir);

        parse(inputFile, outputFile);
    }

    public static void parse(String inputFilename, String outputFilename) {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFilename));
            if (statement != null) {
                System.out.println("Read statement: " + statement);
                Select select = (Select) statement;

                PrintStream printStream;
                if (outputFilename.equals("System.out")) { // only used for debugging
                    printStream = new PrintStream(System.out);
                } else {
                    printStream = new PrintStream(outputFilename);
                }
                QueryPlanExecutor.getInstance().executeStatement(select).dump(printStream);
                DatabaseCatalog.getInstance().reset(); // we reset both the index and the executor at the end
                QueryPlanExecutor.getInstance().reset(); // this is done to make sure that junit doesn't reuse them between tests.
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }
}
