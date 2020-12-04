package eu.europeana.metis.reprocessing.model;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.reprocessing.dao.MetisCoreMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoDestinationMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.solr.client.CompoundSolrClient;
import eu.europeana.metis.solr.connection.SolrClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic configuration of the re-processing operation.
 * <p>Functionality here should be the same for each re-processing.
 * Internally it holds {@link BasicConfiguration#extraConfiguration} that should contain the
 * functionality per re-process operation.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class BasicConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicConfiguration.class);

  private final PropertiesHolder propertiesHolder;
  private final MetisCoreMongoDao metisCoreMongoDao;
  private final MongoSourceMongoDao mongoSourceMongoDao;
  private final MongoDestinationMongoDao mongoDestinationMongoDao;
  private final CompoundSolrClient destinationCompoundSolrClient;
  private final IndexerPool destinationIndexerPool;
  private final Indexer destinationIndexer;
  private final Mode mode;
  private final boolean identityProcess;
  private final boolean clearDatabasesBeforeProcess;
  private final List<String> datasetIdsToProcess;
  private final ExecutablePluginType reprocessBasedOnPluginType;
  private final List<ExecutablePluginType> invalidatePluginTypes;
  private ExtraConfiguration extraConfiguration;

  public BasicConfiguration(PropertiesHolder propertiesHolder)
      throws IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException {
    this.propertiesHolder = propertiesHolder;
    //Create metis core dao only if there aren't any specific datasets to process and mode not
    // POST_PROCESS
    if (propertiesHolder.datasetIdsToProcess == null || propertiesHolder.mode
        .equals(Mode.POST_PROCESS)) {
      metisCoreMongoDao = new MetisCoreMongoDao(propertiesHolder);
    } else {
      metisCoreMongoDao = null;
    }
    mongoSourceMongoDao = new MongoSourceMongoDao(propertiesHolder);
    mongoDestinationMongoDao = new MongoDestinationMongoDao(propertiesHolder);

    IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);
    destinationCompoundSolrClient = new SolrClientProvider<>(indexingSettings.getSolrProperties())
        .createSolrClient();
    IndexerFactory indexerFactory = new IndexerFactory(indexingSettings);
    destinationIndexerPool = new IndexerPool(indexerFactory, 600, 60);
    destinationIndexer = indexerFactory.getIndexer();
    mode = propertiesHolder.mode;
    datasetIdsToProcess = propertiesHolder.datasetIdsToProcess == null ? Collections.emptyList()
        : Arrays.asList(propertiesHolder.datasetIdsToProcess);
    identityProcess = propertiesHolder.identityProcess;
    clearDatabasesBeforeProcess = propertiesHolder.cleanDatabasesBeforeProcess;
    reprocessBasedOnPluginType = propertiesHolder.reprocessBasedOnPluginType;
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

  public CompoundSolrClient getDestinationCompoundSolrClient() {
    return destinationCompoundSolrClient;
  }

  public IndexerPool getDestinationIndexerPool() {
    return destinationIndexerPool;
  }

  public Indexer getDestinationIndexer() {
    return destinationIndexer;
  }

  public ExtraConfiguration getExtraConfiguration() {
    return extraConfiguration;
  }

  public void setExtraConfiguration(ExtraConfiguration extraConfiguration) {
    this.extraConfiguration = extraConfiguration;
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
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
    indexingSettings.setMongoDatabaseName(propertiesHolder.destinationMongoDb);
    if (StringUtils.isEmpty(propertiesHolder.destinationMongoAuthenticationDb) || StringUtils
        .isEmpty(propertiesHolder.destinationMongoUsername) || StringUtils
        .isEmpty(propertiesHolder.destinationMongoPassword)) {
      LOGGER.info(EXECUTION_LOGS_MARKER, "Mongo credentials not provided");
    } else {
      indexingSettings.setMongoCredentials(propertiesHolder.destinationMongoUsername,
          propertiesHolder.destinationMongoPassword,
          propertiesHolder.destinationMongoAuthenticationDb);
    }

    if (propertiesHolder.destinationMongoEnableSSL) {
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
    indexingSettings.setZookeeperChroot(propertiesHolder.destinationZookeeperChroot);
    indexingSettings
        .setZookeeperDefaultCollection(propertiesHolder.destinationZookeeperDefaultCollection);
  }

  public Mode getMode() {
    return mode;
  }

  public List<String> getDatasetIdsToProcess() {
    return datasetIdsToProcess;
  }

  public boolean isIdentityProcess() {
    return identityProcess;
  }

  public boolean isClearDatabasesBeforeProcess() {
    return clearDatabasesBeforeProcess;
  }

  public ExecutablePluginType getReprocessBasedOnPluginType() {
    return reprocessBasedOnPluginType;
  }

  public List<ExecutablePluginType> getInvalidatePluginTypes() {
    return invalidatePluginTypes;
  }

  public void close() throws IOException {
    if (metisCoreMongoDao != null) {
      metisCoreMongoDao.close();
    }
    mongoSourceMongoDao.close();
    mongoDestinationMongoDao.close();
    destinationCompoundSolrClient.close();
    destinationIndexerPool.close();
    destinationIndexer.close();
  }
}
