package eu.europeana.metis.zoho.exception;

public class WikidataAccessException extends Exception {

    private static final long serialVersionUID = 7724261367420984595L;

    public WikidataAccessException(String message, Throwable th) {
        super(message, th);
    }
}
