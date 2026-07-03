package eu.europeana.metis.published.records.harvest.utils;

import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.mongo.connection.MongoProperties.ReadPreferenceValue;
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

  // Mongo metis-core
  private final String[] mongoCoreHosts;
  private final int[] mongoCorePorts;
  private final String mongoCoreAuthenticationDb;
  private final String mongoCoreUsername;
  private final String mongoCorePassword;
  private final boolean mongoCoreEnablessl;
  final String mongoCoreDb;
  private final String mongoCoreApplicationName;

  // eCloud
  public final String ecloudMcsBaseUrl;
  public final String ecloudProvider;
  public final String ecloudUsername;
  public final String ecloudPassword;

  // truststore
  final String truststorePath;
  final String truststorePassword;

  public PropertiesHolder() {

    // Load properties file.
    final Properties properties = new Properties();
    try (final InputStream stream = PropertiesHolder.class.getClassLoader()
        .getResourceAsStream(CONFIGURATION_FILE)) {
      properties.load(stream);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    // Mongo metis-core
    mongoCoreHosts = properties.getProperty("mongo.core.hosts").split(",");
    mongoCorePorts = Arrays.stream(properties.getProperty("mongo.core.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    mongoCoreAuthenticationDb = properties.getProperty("mongo.core.authentication.db");
    mongoCoreUsername = properties.getProperty("mongo.core.username");
    mongoCorePassword = properties.getProperty("mongo.core.password");
    mongoCoreEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.core.enableSSL"));
    mongoCoreDb = properties.getProperty("mongo.core.db");
    mongoCoreApplicationName = properties.getProperty("mongo.core.application.name");

    // eCloud
    ecloudMcsBaseUrl = properties.getProperty("ecloud.mcs.baseUrl");
    ecloudProvider = properties.getProperty("ecloud.provider");
    ecloudUsername = properties.getProperty("ecloud.username");
    ecloudPassword = properties.getProperty("ecloud.password");

    // truststore
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
  }

  public MongoProperties<IllegalArgumentException> getMongoCoreProperties() {
    final MongoProperties<IllegalArgumentException> properties = new MongoProperties<>(
            IllegalArgumentException::new);
    properties.setAllProperties(mongoCoreHosts, mongoCorePorts, mongoCoreAuthenticationDb,
            mongoCoreUsername, mongoCorePassword, mongoCoreEnablessl, ReadPreferenceValue.PRIMARY,
            mongoCoreApplicationName);
    return properties;
  }
}
