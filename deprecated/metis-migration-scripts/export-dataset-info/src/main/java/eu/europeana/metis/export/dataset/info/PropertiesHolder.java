package eu.europeana.metis.export.dataset.info;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class PropertiesHolder {

  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDb;

  public final String metisDatasetUriPrefix;

  public final String truststorePath;
  public final String truststorePassword;

  public final String targetFile;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(Thread.currentThread().getContextClassLoader()
          .getResource(configurationFileName).getFile()));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
    mongoHosts = properties.getProperty("mongo.hosts").split(",");
    mongoPorts = Arrays.stream(properties.getProperty("mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    mongoAuthenticationDb = properties.getProperty("mongo.authentication.db");
    mongoUsername = properties.getProperty("mongo.username");
    mongoPassword = properties.getProperty("mongo.password");
    mongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.enableSSL"));
    mongoDb = properties.getProperty("mongo.db");

    metisDatasetUriPrefix = properties.getProperty("metis.dataset.uri.prefix");

    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");

    targetFile = properties.getProperty("target.file");
  }
}
