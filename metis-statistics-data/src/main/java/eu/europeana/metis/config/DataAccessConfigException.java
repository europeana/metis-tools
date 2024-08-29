package eu.europeana.metis.config;

public class DataAccessConfigException extends Exception {

  public DataAccessConfigException(String message) {
    super(message);
  }

  public DataAccessConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
