package ed.inf.adbs.lightdb;

/**
 * A custom Exception for any parse-related errors
 **/
public class ParseException extends RuntimeException {
    public ParseException(String errorMessage) {
        super(errorMessage);
    }
}
