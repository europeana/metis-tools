package eu.europeana.metis.depublishing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.depublishing.dao.MetisCoreMongoDao;
import eu.europeana.metis.depublishing.dao.MongoDao;
import eu.europeana.metis.depublishing.exception.ProcessingException;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.solr.client.CompoundSolrClient;
import eu.europeana.metis.solr.connection.SolrClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * Basic configuration of the re-processing operation.
 * <p>Functionality here should be the same for each re-processing.
 * Extend this class with a class that should also contain the functionality per re-process operation.</p>
 */
public abstract class Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

  private final PropertiesHolder propertiesHolder;
  private final MetisCoreMongoDao metisCoreMongoDao;

  private final MongoDao mongoDao;
  private final CompoundSolrClient destinationCompoundSolrClient;
  private final IndexerPool destinationIndexerPool;
  private final Indexer destinationIndexer;
  private final Mode mode;
  private final boolean identityProcess;
  private final boolean depublicationEnabled;

  private final List<String> datasetIdsToProcess;
  private final List<String> recordIdsToProcess;

  protected Configuration(PropertiesHolder propertiesHolder)
      throws IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException {
    this.propertiesHolder = propertiesHolder;
    if (CollectionUtils.isEmpty(propertiesHolder.datasetIdsToProcess)) {
      metisCoreMongoDao = new MetisCoreMongoDao(propertiesHolder);
    } else {
      metisCoreMongoDao = null;
    }
    mongoDao = new MongoDao(propertiesHolder);

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
    datasetIdsToProcess = propertiesHolder.datasetIdsToProcess;
    recordIdsToProcess = propertiesHolder.recordIdsToProcess;
    identityProcess = propertiesHolder.identityProcess;
    depublicationEnabled = propertiesHolder.depublicationEnabled;
  }

  public MetisCoreMongoDao getMetisCoreMongoDao() {
    return metisCoreMongoDao;
  }

  public MongoDao getMongoDao() {
    return mongoDao;
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

  public Mode getMode() {
    return mode;
  }

  public List<String> getDatasetIdsToProcess() {
    return datasetIdsToProcess;
  }

  public boolean isIdentityProcess() {
    return identityProcess;
  }

  public boolean isDepublicationEnabled() {
    return depublicationEnabled;
  }


  public abstract ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor();

  public abstract ThrowingTriConsumer<RDF, FullBeanImpl, Configuration> getRdfIndexer();

  public abstract RDF processRDF(RDF rdf);

  public void close() throws IOException {
    if (metisCoreMongoDao != null) {
      metisCoreMongoDao.close();
    }
    mongoDao.close();
    destinationCompoundSolrClient.close();
    destinationIndexerPool.close();
    destinationIndexer.close();
  }

  public List<String> getRecordIdsToProcess() {
    return recordIdsToProcess;
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
    for (int i = 0; i < propertiesHolder.mongoHosts.length; i++) {
      if (propertiesHolder.mongoHosts.length
          == propertiesHolder.mongoPorts.length) {
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.mongoHosts[i],
                propertiesHolder.mongoPorts[i]));
      } else { // Same port for all
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.mongoHosts[i],
                propertiesHolder.mongoPorts[0]));
      }
    }
    indexingSettings.setMongoDatabaseName(propertiesHolder.mongoDb);
    indexingSettings.setMongoTombstoneDatabaseName(propertiesHolder.mongoTombstoneDb);
    if (StringUtils.isEmpty(propertiesHolder.mongoAuthenticationDb) || StringUtils
        .isEmpty(propertiesHolder.mongoUsername) || StringUtils
        .isEmpty(propertiesHolder.mongoPassword) || StringUtils
        .isEmpty(propertiesHolder.mongoTombstoneDb)) {
      LOGGER.info("Mongo credentials not provided");
    } else {
      indexingSettings.setMongoCredentials(propertiesHolder.mongoUsername,
          propertiesHolder.mongoPassword,
          propertiesHolder.mongoAuthenticationDb);

    }

    if (propertiesHolder.mongoEnableSSL) {
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

  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {

    R apply(T t, U u) throws ProcessingException;
  }

  @FunctionalInterface
  public interface ThrowingTriConsumer<K, V, S> {

    void accept(K k, V v, S s) throws IndexingException;
  }

  @FunctionalInterface
  public interface ThrowingQuadConsumer<K, V, S, T> {

    void accept(K k, V v, S s, T t) throws ProcessingException;
  }
}
