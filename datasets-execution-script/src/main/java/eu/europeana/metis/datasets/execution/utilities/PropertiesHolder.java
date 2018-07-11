package eu.europeana.metis.datasets.execution.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
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
  public final String metisAuthenticationHost;
  public final int monitorIntervalInSecs;
  public final String enforcedPluginType;
  public final String metisUsername;
  public final String metisPassword;
  public final int numberOfDatasetsToProcess;
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
    metisAuthenticationHost = properties.getProperty("metis.authentication.host");
    monitorIntervalInSecs = Integer.parseInt(properties.getProperty("monitor.interval.in.secs"));
    enforcedPluginType = properties.getProperty("enforced.plugin.type");
    metisUsername = properties.getProperty("metis.username");
    metisPassword = properties.getProperty("metis.password");
    numberOfDatasetsToProcess = StringUtils.isNotBlank(properties.getProperty("number.of.datasets.to.process")) ? Integer
        .parseInt(properties.getProperty("number.of.datasets.process")) : Integer.MAX_VALUE;
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
