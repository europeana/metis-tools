package eu.europeana.metis.remove.utils;

import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.mongo.MongoProperties;
import eu.europeana.metis.mongo.MongoProperties.ReadPreferenceValue;
import eu.europeana.metis.utils.InetAddressUtil;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
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

  // eCloud
  public final String ecloudMcsBaseUrl;
  public final String ecloudProvider;
  public final String ecloudUsername;
  public final String ecloudPassword;

  // truststore
  final String truststorePath;
  final String truststorePassword;

  // Mongo publish
  private final String[] publishMongoHosts;
  private final int[] publishMongoPorts;
  private final String publishMongoAuthenticationDb;
  private final String publishMongoUsername;
  private final String publishMongoPassword;
  private final boolean publishMongoEnablessl;
  private final String publishMongoDb;

  // Solr/Zookeeper publish
  private final String[] publishSolrHosts;
  private final String[] publishZookeeperHosts;
  private final int[] publishZookeeperPorts;
  private final String publishZookeeperChroot;
  private final String publishZookeeperDefaultCollection;

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

    // eCloud
    ecloudMcsBaseUrl = properties.getProperty("ecloud.mcs.baseUrl");
    ecloudProvider = properties.getProperty("ecloud.provider");
    ecloudUsername = properties.getProperty("ecloud.username");
    ecloudPassword = properties.getProperty("ecloud.password");

    // truststore
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");

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

    // Solr/Zookeeper publish
    publishSolrHosts = properties.getProperty("solr.publish.hosts").split(",");
    publishZookeeperHosts = properties.getProperty("zookeeper.publish.hosts").split(",");
    publishZookeeperPorts = Arrays
        .stream(properties.getProperty("zookeeper.publish.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    publishZookeeperChroot = properties.getProperty("zookeeper.publish.chroot");
    publishZookeeperDefaultCollection = properties
        .getProperty("zookeeper.publish.defaultCollection");
  }

  public MongoProperties<IllegalArgumentException> getMongoCoreProperties() {
    final MongoProperties<IllegalArgumentException> properties = new MongoProperties<>(
            IllegalArgumentException::new);
    properties.setAllProperties(mongoCoreHosts, mongoCorePorts, mongoCoreAuthenticationDb,
            mongoCoreUsername, mongoCorePassword, mongoCoreEnablessl, ReadPreferenceValue.getDefault());
    return properties;
  }

  public IndexingSettings getPublishIndexingSettings()
      throws IndexingException, URISyntaxException {
    final IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);
    indexingSettings.setRecordRedirectDatabaseName("assumed_to_not_be_needed");
    return indexingSettings;
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
    indexingSettings.getMongoProperties().setAllProperties(publishMongoHosts,
            publishMongoPorts, publishMongoAuthenticationDb, publishMongoUsername,
            publishMongoPassword, publishMongoEnablessl, ReadPreferenceValue.getDefault());
    indexingSettings.setMongoDatabaseName(publishMongoDb);
  }

  private void prepareSolrSettings(IndexingSettings indexingSettings)
      throws URISyntaxException, SetupRelatedIndexingException {
    for (String instance : publishSolrHosts) {
      indexingSettings.addSolrHost(new URI(instance + publishZookeeperDefaultCollection));
    }
  }

  private void prepareZookeeperSettings(IndexingSettings indexingSettings)
      throws SetupRelatedIndexingException {
    final List<InetSocketAddress> addresses = new InetAddressUtil<>(
            SetupRelatedIndexingException::new)
            .getAddressesFromHostsAndPorts(publishZookeeperHosts, publishZookeeperPorts);
    for (InetSocketAddress address : addresses) {
      indexingSettings.addZookeeperHost(address);
    }
    indexingSettings.setZookeeperChroot(publishZookeeperChroot);
    indexingSettings.setZookeeperDefaultCollection(publishZookeeperDefaultCollection);
  }
}
