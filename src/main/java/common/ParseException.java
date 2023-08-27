package common;

public class ParseException extends Exception{

    public ParseException(String string) {
        super(string);
    }

    public ParseException(Exception e) {
        super(e);
    }


}
