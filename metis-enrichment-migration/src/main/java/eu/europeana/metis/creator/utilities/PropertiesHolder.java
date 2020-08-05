package eu.europeana.metis.creator.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);

  public final String[] sourceMongoHosts;
  public final int[] sourceMongoPorts;
  public final String sourceMongoAuthenticationDb;
  public final String sourceMongoUsername;
  public final String sourceMongoPassword;
  public final boolean sourceMongoEnablessl;
  public final String sourceMongoDb;

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
      LOGGER.info("Will try to load {} properties file",
          filePathInResources);
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

    sourceMongoHosts = properties.getProperty("source.mongo.hosts").split(",");
    sourceMongoPorts = Arrays.stream(properties.getProperty("source.mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    sourceMongoAuthenticationDb = properties.getProperty("source.mongo.authentication.db");
    sourceMongoUsername = properties.getProperty("source.mongo.username");
    sourceMongoPassword = properties.getProperty("source.mongo.password");
    sourceMongoEnablessl = Boolean.parseBoolean(properties.getProperty("source.mongo.enableSSL"));
    sourceMongoDb = properties.getProperty("source.mongo.db");

    destinationMongoHosts = properties.getProperty("destination.mongo.hosts").split(",");
    destinationMongoPorts = Arrays.stream(properties.getProperty("destination.mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationMongoAuthenticationDb = properties.getProperty("destination.mongo.authentication.db");
    destinationMongoUsername = properties.getProperty("destination.mongo.username");
    destinationMongoPassword = properties.getProperty("destination.mongo.password");
    destinationMongoEnablessl = Boolean.parseBoolean(properties.getProperty("destination.mongo.enableSSL"));
    destinationMongoDb = properties.getProperty("destination.mongo.db");
  }
}
