package eu.europeana.metis.reprocessing.exception;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-17
 */
public class ProcessingException extends Exception {

  /**
   * Required for implementations of {@link java.io.Serializable}.
   **/
  private static final long serialVersionUID = 486509140109072537L;

  /**
   * Constructor.
   *
   * @param message The message. Can be null.
   */
  public ProcessingException(String message) {
    super(message);
  }

  /**
   * Constructor.
   *
   * @param message The message. Can be null.
   * @param cause The cause. Can be null.
   */
  public ProcessingException(String message, Exception cause) {
    super(message, cause);
  }
}
