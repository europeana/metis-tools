package eu.europeana.migration.metis.utils;

/**
 * This class is responsible for logging. All code in this project should use these methods for
 * logging, so that when a different logging platform/method is chosen, only this class will need to
 * be changed.
 * 
 * @author jochen
 *
 */
public final class LogUtils {

  private LogUtils() {}

  /**
   * Log an information message.
   * 
   * @param message The message to log.
   */
  public static void logInfoMessage(String message) {
    System.out.println(message);
  }

  /**
   * Log an exception.
   * 
   * @param exception The exception to log.
   */
  public static void logException(Throwable exception) {
    System.out.println("An exception occurred:");
    exception.printStackTrace();
  }
}
