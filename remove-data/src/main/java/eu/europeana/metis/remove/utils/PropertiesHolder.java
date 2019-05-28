package eu.europeana.metis.remove.utils;

import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

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
  final String[] mongoCoreHosts;
  final int[] mongoCorePorts;
  final String mongoCoreAuthenticationDb;
  final String mongoCoreUsername;
  final String mongoCorePassword;
  final boolean mongoCoreEnablessl;
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

  public IndexingSettings getPublishIndexingSettings()
      throws IndexingException, URISyntaxException {
    final IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);
    return indexingSettings;
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
    for (int i = 0; i < publishMongoHosts.length; i++) {
      if (publishMongoHosts.length == publishMongoPorts.length) {
        indexingSettings
            .addMongoHost(new InetSocketAddress(publishMongoHosts[i], publishMongoPorts[i]));
      } else { // Same port for all
        indexingSettings
            .addMongoHost(new InetSocketAddress(publishMongoHosts[i], publishMongoPorts[0]));
      }
    }
    indexingSettings.setMongoDatabaseName(publishMongoDb);
    if (!StringUtils.isEmpty(publishMongoAuthenticationDb) && !StringUtils
        .isEmpty(publishMongoUsername) && !StringUtils.isEmpty(publishMongoPassword)) {
      indexingSettings.setMongoCredentials(publishMongoUsername, publishMongoPassword,
          publishMongoAuthenticationDb);
    }

    if (publishMongoEnablessl) {
      indexingSettings.setMongoEnableSsl();
    }
  }

  private void prepareSolrSettings(IndexingSettings indexingSettings)
      throws URISyntaxException, SetupRelatedIndexingException {
    for (String instance : publishSolrHosts) {
      indexingSettings.addSolrHost(new URI(instance + publishZookeeperDefaultCollection));
    }
  }

  private void prepareZookeeperSettings(IndexingSettings indexingSettings)
      throws SetupRelatedIndexingException {
    for (int i = 0; i < publishZookeeperHosts.length; i++) {
      if (publishZookeeperHosts.length == publishZookeeperPorts.length) {
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(publishZookeeperHosts[i], publishZookeeperPorts[i]));
      } else { // Same port for all
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(publishZookeeperHosts[i], publishZookeeperPorts[0]));
      }
    }
    indexingSettings.setZookeeperChroot(publishZookeeperChroot);
    indexingSettings.setZookeeperDefaultCollection(publishZookeeperDefaultCollection);
  }
}
