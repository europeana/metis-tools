package eu.europeana.metis.datasets.execution.utilities;

import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import java.net.SocketException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.ResourceAccessException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-07-24
 */
public final class ExternalRequestUtilMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalRequestUtilMigration.class);
  private static final int MAX_RETRIES = Integer.MAX_VALUE;
  private static final int SLEEP_TIMEOUT = 2000;

  private ExternalRequestUtilMigration() {
  }

  /**
   * Retries a request to an external service like a database. This method is meant to be called
   * when request throws a {@link RuntimeException} that contains a {@link SocketException} cause
   * with message "Connection reset". This method was intentionally implemented for the above
   * described issue that is caused in the Bluemix Cloud Foundry environment. Some examples are:
   * <ul>
   * <li>{@link MongoSocketException}</li> From a Mongo request
   * <li>{@link MongoSecurityException}</li> From a Mongo request
   * <li>{@link ResourceAccessException}</li> From an HTTP request
   * </ul>
   *
   * @param supplier the respective supplier encapsulating the external request
   * @return the expected object as a result of the external request
   */
  public static <R> R retryableExternalRequest(
      Supplier<R> supplier) {
    int retryCounter = 0;

    do {
      try {
        return supplier.get();
      } catch (RuntimeException e) {
        retryCounter++;
        //Re-throw if max retries exceeded.
        if (retryCounter > MAX_RETRIES) {
          throw e;
        }
        LOGGER
            .warn(String.format("External request has failed! Retrying in %sms", SLEEP_TIMEOUT), e);
        try {
          Thread.sleep(SLEEP_TIMEOUT);
        } catch (InterruptedException ex) {
          LOGGER.warn("Thread was interrupted while waiting for retry.", ex);
          Thread.currentThread().interrupt();
          return null;
        }
      }
    } while (true);
  }
}
