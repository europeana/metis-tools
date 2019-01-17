package eu.europeana.metis.remove.dataset.utils;

import java.io.IOException;
import java.io.InputStream;
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

  private static final String CONFIGURATION_FILE = "application.properties";

  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDb;

  public final String ecloudMcsBaseUrl;
  public final String ecloudProvider;
  public final String ecloudUsername;
  public final String ecloudPassword;

  public final String truststorePath;
  public final String truststorePassword;

  public PropertiesHolder() {
    Properties properties = new Properties();
    try (final InputStream stream = PropertiesHolder.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE)) {
      properties.load(stream);
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

    ecloudMcsBaseUrl = properties.getProperty("ecloud.baseUrl");
    ecloudProvider = properties.getProperty("ecloud.provider");
    ecloudUsername = properties.getProperty("ecloud.username");
    ecloudPassword = properties.getProperty("ecloud.password");

    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
  }
}
