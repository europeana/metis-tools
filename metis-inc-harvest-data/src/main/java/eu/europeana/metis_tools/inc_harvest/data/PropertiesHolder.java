package eu.europeana.metis_tools.inc_harvest.data;

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
  private final String mongoCoreDb;
  private final String mongoCoreApplicationName;

  // truststore
  private final String truststorePath;
  private final String truststorePassword;

  // Mongo preview
  private final String[] previewMongoHosts;
  private final int[] previewMongoPorts;
  private final String previewMongoAuthenticationDb;
  private final String previewMongoUsername;
  private final String previewMongoPassword;
  private final boolean previewMongoEnablessl;
  private final String previewMongoDb;
  private final String previewMongoApplicationName;

  // Mongo publish
  private final String[] publishMongoHosts;
  private final int[] publishMongoPorts;
  private final String publishMongoAuthenticationDb;
  private final String publishMongoUsername;
  private final String publishMongoPassword;
  private final boolean publishMongoEnablessl;
  private final String publishMongoDb;
  private final String publishMongoApplicationName;

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

    // truststore
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");

    // Mongo publish
    previewMongoHosts = properties.getProperty("mongo.preview.hosts").split(",");
    previewMongoPorts = Arrays
            .stream(properties.getProperty("mongo.preview.port").split(","))
            .mapToInt(Integer::parseInt).toArray();
    previewMongoAuthenticationDb = properties
            .getProperty("mongo.preview.authentication.db");
    previewMongoUsername = properties.getProperty("mongo.preview.username");
    previewMongoPassword = properties.getProperty("mongo.preview.password");
    previewMongoEnablessl = Boolean
            .parseBoolean(properties.getProperty("mongo.preview.enableSSL"));
    previewMongoDb = properties.getProperty("mongo.preview.db");
    previewMongoApplicationName = properties.getProperty("mongo.preview.application.name");

    // Mongo publish
    publishMongoHosts = properties.getProperty("mongo.publish.hosts").split(",");
    publishMongoPorts = Arrays
            .stream(properties.getProperty("mongo.publish.port").split(","))
            .mapToInt(Integer::parseInt).toArray();
    publishMongoAuthenticationDb = properties
            .getProperty("mongo.publish.authentication.db");
    publishMongoUsername = properties.getProperty("mongo.publish.username");
    publishMongoPassword = properties.getProperty("mongo.publish.password");
    publishMongoEnablessl = Boolean
            .parseBoolean(properties.getProperty("mongo.publish.enableSSL"));
    publishMongoDb = properties.getProperty("mongo.publish.db");
    publishMongoApplicationName = properties.getProperty("mongo.publish.application.name");
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public MongoProperties<IllegalArgumentException> getMongoCoreProperties() {
    final MongoProperties<IllegalArgumentException> properties = new MongoProperties<>(
            IllegalArgumentException::new);
    properties.setAllProperties(mongoCoreHosts, mongoCorePorts, mongoCoreAuthenticationDb,
            mongoCoreUsername, mongoCorePassword, mongoCoreEnablessl, ReadPreferenceValue.PRIMARY,
            mongoCoreApplicationName);
    return properties;
  }

  public String getMongoCoreDb() {
    return mongoCoreDb;
  }

  public MongoProperties<IllegalArgumentException> getMongoPreviewProperties() {
    final MongoProperties<IllegalArgumentException> properties = new MongoProperties<>(
            IllegalArgumentException::new);
    properties.setAllProperties(previewMongoHosts, previewMongoPorts, previewMongoAuthenticationDb,
            previewMongoUsername, previewMongoPassword, previewMongoEnablessl,
            ReadPreferenceValue.PRIMARY, previewMongoApplicationName);
    return properties;
  }

  public String getPreviewMongoDb() {
    return previewMongoDb;
  }

  public MongoProperties<IllegalArgumentException> getMongoPublishProperties() {
    final MongoProperties<IllegalArgumentException> properties = new MongoProperties<>(
            IllegalArgumentException::new);
    properties.setAllProperties(publishMongoHosts, publishMongoPorts, publishMongoAuthenticationDb,
            publishMongoUsername, publishMongoPassword, publishMongoEnablessl,
            ReadPreferenceValue.PRIMARY, publishMongoApplicationName);
    return properties;
  }

  public String getPublishMongoDb() {
    return publishMongoDb;
  }
}
