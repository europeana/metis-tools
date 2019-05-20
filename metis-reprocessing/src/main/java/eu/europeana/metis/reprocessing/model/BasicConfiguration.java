package eu.europeana.metis.reprocessing.model;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.reprocessing.dao.MetisCoreMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoDestinationMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class BasicConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicConfiguration.class);

  private final PropertiesHolder propertiesHolder;
  private final MetisCoreMongoDao metisCoreMongoDao;
  private final MongoSourceMongoDao mongoSourceMongoDao;
  private final MongoDestinationMongoDao mongoDestinationMongoDao;
  private final Indexer indexer;
  private final List<ExecutablePluginType> invalidatePluginTypes;
  private ExtraConfiguration extraConfiguration;

  public BasicConfiguration(PropertiesHolder propertiesHolder)
      throws TrustStoreConfigurationException, IndexingException, URISyntaxException {
    this.propertiesHolder = propertiesHolder;
    metisCoreMongoDao = new MetisCoreMongoDao(propertiesHolder);
    mongoSourceMongoDao = new MongoSourceMongoDao(propertiesHolder);
    mongoDestinationMongoDao = new MongoDestinationMongoDao(propertiesHolder);

    IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);
    IndexerFactory indexerFactory = new IndexerFactory(indexingSettings);
    indexer = indexerFactory.getIndexer();
    invalidatePluginTypes = propertiesHolder.invalidatePluginTypes;
  }

  public MetisCoreMongoDao getMetisCoreMongoDao() {
    return metisCoreMongoDao;
  }

  public MongoSourceMongoDao getMongoSourceMongoDao() {
    return mongoSourceMongoDao;
  }

  public MongoDestinationMongoDao getMongoDestinationMongoDao() {
    return mongoDestinationMongoDao;
  }

  public Indexer getIndexer() {
    return indexer;
  }

  public ExtraConfiguration getExtraConfiguration() {
    return extraConfiguration;
  }

  public void setExtraConfiguration(
      ExtraConfiguration extraConfiguration) {
    this.extraConfiguration = extraConfiguration;
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings)
      throws IndexingException {
    for (int i = 0; i < propertiesHolder.destinationMongoHosts.length; i++) {
      if (propertiesHolder.destinationMongoHosts.length
          == propertiesHolder.destinationMongoPorts.length) {
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.destinationMongoHosts[i],
                propertiesHolder.destinationMongoPorts[i]));
      } else { // Same port for all
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.destinationMongoHosts[i],
                propertiesHolder.destinationMongoPorts[0]));
      }
    }
    indexingSettings
        .setMongoDatabaseName(propertiesHolder.destinationMongoDb);
    if (StringUtils.isEmpty(propertiesHolder.destinationMongoAuthenticationDb) || StringUtils
        .isEmpty(propertiesHolder.destinationMongoUsername) || StringUtils
        .isEmpty(propertiesHolder.destinationMongoPassword)) {
      LOGGER.info("Mongo credentials not provided");
    } else {
      indexingSettings.setMongoCredentials(
          propertiesHolder.destinationMongoUsername,
          propertiesHolder.destinationMongoPassword,
          propertiesHolder.destinationMongoAuthenticationDb);
    }

    if (propertiesHolder.destinationMongoEnablessl) {
      indexingSettings.setMongoEnableSsl();
    }
  }

  private void prepareSolrSettings(IndexingSettings indexingSettings)
      throws URISyntaxException, SetupRelatedIndexingException {
    for (String instance : propertiesHolder.destinationSolrHosts) {
      indexingSettings
          .addSolrHost(new URI(instance + propertiesHolder.destinationZookeeperDefaultCollection));
    }
  }

  private void prepareZookeeperSettings(IndexingSettings indexingSettings)
      throws SetupRelatedIndexingException {
    for (int i = 0; i < propertiesHolder.destinationZookeeperHosts.length; i++) {
      if (propertiesHolder.destinationZookeeperHosts.length
          == propertiesHolder.destinationZookeeperPorts.length) {
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(propertiesHolder.destinationZookeeperHosts[i],
                propertiesHolder.destinationZookeeperPorts[i]));
      } else { // Same port for all
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(propertiesHolder.destinationZookeeperHosts[i],
                propertiesHolder.destinationZookeeperPorts[0]));
      }
    }
    indexingSettings
        .setZookeeperChroot(propertiesHolder.destinationZookeeperChroot);
    indexingSettings
        .setZookeeperDefaultCollection(propertiesHolder.destinationZookeeperDefaultCollection);
  }

  public List<ExecutablePluginType> getInvalidatePluginTypes() {
    return invalidatePluginTypes;
  }

  public void close() throws IOException {
    metisCoreMongoDao.close();
    mongoSourceMongoDao.close();
    mongoDestinationMongoDao.close();
    if (extraConfiguration != null) {
      extraConfiguration.close();
    }
    indexer.close();
  }
}
