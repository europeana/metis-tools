package eu.europeana.metis.performance.metric.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

/**
 * Contains all properties that are required for execution.
 * During construction will read properties from the specified file from the classpath.
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);

  //Metis Core Mongo
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] metisCoreMongoHosts;
  public final int[] metisCoreMongoPorts;
  public final String metisCoreMongoAuthenticationDb;
  public final String metisCoreMongoUsername;
  public final String metisCoreMongoPassword;
  public final boolean metisCoreMongoEnableSSL;
  public final String metisCoreMongoDb;
  public final int metisCoreConnectionPoolSize;

  public final Properties properties = new Properties();

  public PropertiesHolder(String configurationFileName) {
    final URL resource = getClass().getClassLoader().getResource(configurationFileName);
    final String filePathInResources = resource == null ? null : resource.getFile();
    String filePath;
    if (filePathInResources != null && new File(filePathInResources).exists()) {
      LOGGER.info("Will try to load {} properties file", filePathInResources);
      filePath = filePathInResources;
    } else {
      LOGGER.info(
          "{} properties file does NOT exist, probably running in standalone .jar mode where the properties file should be on the same directory "
              + "as the .jar file is. Will try to load {} properties file", filePathInResources,
          configurationFileName);
      filePath = configurationFileName;
    }
    try {
      properties.load(new FileInputStream(filePath));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    //Metis Core Mongo
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
    metisCoreMongoHosts = properties.getProperty("mongo.metis.core.hosts").split(",");

    if (StringUtils.isBlank(properties.getProperty("mongo.metis.core.port"))) {
      metisCoreMongoPorts = null;
    } else {
      metisCoreMongoPorts = Arrays
          .stream(properties.getProperty("mongo.metis.core.port").split(","))
          .mapToInt(Integer::parseInt).toArray();
    }
    metisCoreMongoAuthenticationDb = properties.getProperty("mongo.metis.core.authentication.db");
    metisCoreMongoUsername = properties.getProperty("mongo.metis.core.username");
    metisCoreMongoPassword = properties.getProperty("mongo.metis.core.password");
    metisCoreMongoEnableSSL = Boolean
        .parseBoolean(properties.getProperty("mongo.metis.core.enableSSL"));
    metisCoreMongoDb = properties.getProperty("mongo.metis.core.db");
    metisCoreConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.metis.core.connection.pool.size"), 50);

  }
}
