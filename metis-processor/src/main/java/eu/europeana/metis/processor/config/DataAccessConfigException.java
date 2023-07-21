package eu.europeana.metis.processor.config;

public class DataAccessConfigException extends Exception {

  public DataAccessConfigException(String message) {
    super(message);
  }

  public DataAccessConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
