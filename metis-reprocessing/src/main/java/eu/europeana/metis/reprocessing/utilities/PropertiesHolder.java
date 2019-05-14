package eu.europeana.metis.reprocessing.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);
  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");
  public static final Marker STATISTICS_LOGS_MARKER = MarkerFactory.getMarker("STATISTICS_LOGS");

  public final String truststorePath;
  public final String truststorePassword;
  public final String[] metisCoreMongoHosts;
  public final int[] metisCoreMongoPorts;
  public final String metisCoreMongoAuthenticationDb;
  public final String metisCoreMongoUsername;
  public final String metisCoreMongoPassword;
  public final boolean metisCoreMongoEnablessl;
  public final String metisCoreMongoDb;
  //Mongo Source
  public final String[] sourceMongoHosts;
  public final int[] sourceMongoPorts;
  public final String sourceMongoAuthenticationDb;
  public final String sourceMongoUsername;
  public final String sourceMongoPassword;
  public final boolean sourceMongoEnablessl;
  public final String sourceMongoDb;
  //Mongo Destination
  public final String[] destinationMongoHosts;
  public final int[] destinationMongoPorts;
  public final String destinationMongoAuthenticationDb;
  public final String destinationMongoUsername;
  public final String destinationMongoPassword;
  public final boolean destinationMongoEnablessl;
  public final String destinationMongoDb;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    final URL resource = getClass().getClassLoader().getResource(configurationFileName);
    final String filePathInResources = resource == null ? null : resource.getFile();
    String filePath;
    if (filePathInResources != null && new File(filePathInResources).exists()) {
      LOGGER.info("Will try to load {} properties file", filePathInResources);
      filePath = filePathInResources;
    } else {
      LOGGER.info(
          "{} properties file does NOT exist, probably running in standalone .jar mode where the properties file should be on the same directory "
              + "as the .jar file is. Will try to load {} properties file",
          filePathInResources, configurationFileName);
      filePath = configurationFileName;
    }
    try {
      properties.load(new FileInputStream(filePath));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
    metisCoreMongoHosts = properties.getProperty("mongo.metis.core.hosts").split(",");
    metisCoreMongoPorts = Arrays.stream(properties.getProperty("mongo.metis.core.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    metisCoreMongoAuthenticationDb = properties.getProperty("mongo.metis.core.authentication.db");
    metisCoreMongoUsername = properties.getProperty("mongo.metis.core.username");
    metisCoreMongoPassword = properties.getProperty("mongo.metis.core.password");
    metisCoreMongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.metis.core.enableSSL"));
    metisCoreMongoDb = properties.getProperty("mongo.metis.core.db");

    //Mongo Source
    sourceMongoHosts = properties.getProperty("mongo.source.hosts").split(",");
    sourceMongoPorts = Arrays.stream(properties.getProperty("mongo.source.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    sourceMongoAuthenticationDb = properties.getProperty("mongo.source.authentication.db");
    sourceMongoUsername = properties.getProperty("mongo.source.username");
    sourceMongoPassword = properties.getProperty("mongo.source.password");
    sourceMongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.source.enableSSL"));
    sourceMongoDb = properties.getProperty("mongo.source.db");

    //Mongo Destination
    destinationMongoHosts = properties.getProperty("mongo.destination.hosts").split(",");
    destinationMongoPorts = Arrays.stream(properties.getProperty("mongo.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationMongoAuthenticationDb = properties.getProperty("mongo.destination.authentication.db");
    destinationMongoUsername = properties.getProperty("mongo.destination.username");
    destinationMongoPassword = properties.getProperty("mongo.destination.password");
    destinationMongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.destination.enableSSL"));
    destinationMongoDb = properties.getProperty("mongo.destination.db");
  }
}
