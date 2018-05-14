package eu.europeana.metis.datasets.execution.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class PropertiesHolder {

  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");
  public static final Marker FINAL_DATASET_STATUS = MarkerFactory.getMarker("FINAL_DATASET_STATUS");
  public static final Marker PROCESSED_DATASETS = MarkerFactory.getMarker("PROCESSED_DATASETS");

  public final String organizationId;
  public final String metisCoreHost;
  public final int monitorIntervalInSecs;
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDb;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(Thread.currentThread().getContextClassLoader()
          .getResource(configurationFileName).getFile()));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
    organizationId = properties.getProperty("organization.id");
    metisCoreHost = properties.getProperty("metis.core.host");
    monitorIntervalInSecs = Integer.parseInt(properties.getProperty("monitor.interval.in.secs"));
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
    mongoHosts = properties.getProperty("mongo.hosts").split(",");
    mongoPorts = Arrays.stream(properties.getProperty("mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    mongoAuthenticationDb = properties.getProperty("mongo.authentication.db");
    mongoUsername = properties.getProperty("mongo.username");
    mongoPassword = properties.getProperty("mongo.password");
    mongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.enableSSL"));
    mongoDb = properties.getProperty("mongo.db");
  }
}
