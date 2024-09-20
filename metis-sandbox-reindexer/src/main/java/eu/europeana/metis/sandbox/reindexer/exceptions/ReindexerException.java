package eu.europeana.metis.sandbox.reindexer.exceptions;

public class ReindexerException extends RuntimeException {

  public ReindexerException(String message) {
    super(message);
  }

  public ReindexerException(String message, Throwable cause) {
    super(message, cause);
  }
}
