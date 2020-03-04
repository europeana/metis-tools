package eu.europeana.metis.redirects.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-02-25
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);
  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");

  public final int rowsPerRequest;
  public final List<String> datasetIdsToKeep;
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDb;
  public final String mongoDbRedirects;

  public final String[] recordsMongoHosts;
  public final int[] recordsMongoPorts;
  public final String recordsMongoAuthenticationDb;
  public final String recordsMongoUsername;
  public final String recordsMongoPassword;
  public final boolean recordsMongoEnablessl;
  public final String recordsMongoDb;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    final URL resource = getClass().getClassLoader().getResource(configurationFileName);
    final String filePathInResources = resource == null ? null : resource.getFile();
    String filePath;
    if (filePathInResources != null && new File(filePathInResources).exists()) {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Will try to load {} properties file",
          filePathInResources);
      filePath = filePathInResources;
    } else {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
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

    rowsPerRequest = Integer.parseInt(properties.getProperty("rows.per.request"));
    datasetIdsToKeep = Arrays.stream(properties.getProperty("dataset.ids.to.keep").split(","))
        .filter(StringUtils::isNotBlank).collect(Collectors.toList());
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
    mongoDbRedirects = properties.getProperty("mongo.db.redirects");

    recordsMongoHosts = properties.getProperty("records.mongo.hosts").split(",");
    recordsMongoPorts = Arrays.stream(properties.getProperty("records.mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    recordsMongoAuthenticationDb = properties.getProperty("records.mongo.authentication.db");
    recordsMongoUsername = properties.getProperty("records.mongo.username");
    recordsMongoPassword = properties.getProperty("records.mongo.password");
    recordsMongoEnablessl = Boolean.parseBoolean(properties.getProperty("records.mongo.enableSSL"));
    recordsMongoDb = properties.getProperty("records.mongo.db");
  }
}
