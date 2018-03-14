package eu.europeana.metis.tools.dataset.migration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-14
 */
public class PropertiesHolder {

  public static final Marker FAILED_CSV_LINES_MARKER = MarkerFactory.getMarker("FAILED_CSV_LINES");
  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");
  public static final Marker SUCCESSFULL_DATASET_IDS = MarkerFactory
      .getMarker("SUCCESSFULL_DATASET_IDS");

  public final String organizationId;
  public final String organizationName;
  public final String userId;
  public final String datasetsCsvPath;
  public final Mode mode;
  public final String datasetIdsPath;

  public final String truststorePath;
  public final String truststorePassword;
  public static final String SUGARCRM_DATE_FORMAT = "dd-MM-yyyy HH:mm";
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
    organizationName = properties.getProperty("organization.name");
    userId = properties.getProperty("user.id");
    datasetsCsvPath = properties.getProperty("datasets.csv.path");
    mode = Mode.getModeFromEnumName(properties.getProperty("mode"));
    datasetIdsPath = properties.getProperty("dataset.ids.path");
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
