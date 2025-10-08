package eu.europeana.metis.removal.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.removal.dao.MetisCoreMongoDao;
import eu.europeana.metis.removal.dao.MongoSourceMongoDao;
import eu.europeana.metis.removal.exception.ProcessingException;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.solr.client.CompoundSolrClient;
import eu.europeana.metis.solr.connection.SolrClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * Basic configuration of the remove records operation.
 * <p>Functionality here should be the same for each re-processing.
 * Extend this class with a class that should also contain the functionality per re-process operation.</p>
 *
 */
public abstract class Configuration implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

  private final PropertiesHolder propertiesHolder;
  private final MetisCoreMongoDao metisCoreMongoDao;
  private final MongoSourceMongoDao mongoSourceMongoDao;

  private final CompoundSolrClient destinationCompoundSolrClient;
  private final IndexerPool destinationIndexerPool;
  private final Indexer destinationIndexer;
  private final Mode mode;
  private final boolean identityProcess;

  private final List<String> datasetIdsToProcess;
  private final List<String> recordIdsToProcess;

  protected Configuration(PropertiesHolder propertiesHolder)
      throws IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException, IOException {
    this.propertiesHolder = propertiesHolder;
    //Create metis core dao only if there aren't any specific datasets to process and mode not
    if (CollectionUtils.isEmpty(propertiesHolder.datasetIdsToProcess)) {
      metisCoreMongoDao = new MetisCoreMongoDao(propertiesHolder);
    } else {
      metisCoreMongoDao = null;
    }
    mongoSourceMongoDao = new MongoSourceMongoDao(propertiesHolder);

    IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);
    destinationCompoundSolrClient = new SolrClientProvider<>(indexingSettings.getSolrProperties())
        .createSolrClient();
    IndexerFactory<FullBeanImpl> indexerFactory = IndexerFactory.create(indexingSettings);
    destinationIndexerPool = new IndexerPool(indexerFactory, 600, 60);
    destinationIndexer = indexerFactory.getIndexer();
    mode = propertiesHolder.mode;
    datasetIdsToProcess = propertiesHolder.datasetIdsToProcess;
    recordIdsToProcess = readFileToListString(propertiesHolder.recordIdsToProcess);
    identityProcess = propertiesHolder.identityProcess;
  }

  /**
   * Read file to List<String>.
   *
   * @param file the file
   * @return the string
   * @throws IOException the io exception
   */
  public static List<String> readFileToListString(String file) throws IOException {
    ClassLoader classLoader = Configuration.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(file);
    if (inputStream == null) {
      throw new IOException("Failed reading file " + file);
    }
    return new BufferedReader(new InputStreamReader(inputStream)).lines().toList();
  }

  public MetisCoreMongoDao getMetisCoreMongoDao() {
    return metisCoreMongoDao;
  }

  public MongoSourceMongoDao getMongoSourceMongoDao() {
    return mongoSourceMongoDao;
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

  public abstract ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor();

  public abstract ThrowingTriConsumer<RDF, FullBeanImpl, Configuration> getRdfIndexer();

  public abstract RDF processRDF(RDF rdf);

  public List<String> getRecordIdsToProcess() {
    return recordIdsToProcess;
  }

  @Override
  public void close() throws IOException {
    if (metisCoreMongoDao != null) {
      metisCoreMongoDao.close();
    }
    mongoSourceMongoDao.close();
    destinationCompoundSolrClient.close();
    destinationIndexerPool.close();
    destinationIndexer.close();
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
    for (int i = 0; i < propertiesHolder.sourceMongoHosts.length; i++) {
      if (propertiesHolder.sourceMongoHosts.length
          == propertiesHolder.sourceMongoPorts.length) {
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.sourceMongoHosts[i],
                propertiesHolder.sourceMongoPorts[i]));
      } else { // Same port for all
        indexingSettings.addMongoHost(
            new InetSocketAddress(propertiesHolder.sourceMongoHosts[i],
                propertiesHolder.sourceMongoPorts[0]));
      }
    }
    indexingSettings.setMongoDatabaseName(propertiesHolder.sourceMongoDb);
    indexingSettings.setMongoTombstoneDatabaseName(propertiesHolder.sourceMongoDb);
    if (StringUtils.isEmpty(propertiesHolder.sourceMongoAuthenticationDb) || StringUtils
        .isEmpty(propertiesHolder.sourceMongoUsername) || StringUtils
        .isEmpty(propertiesHolder.sourceMongoPassword) || StringUtils
        .isEmpty(propertiesHolder.sourceMongoDb)) {
      LOGGER.info("Mongo credentials not provided");
    } else {
      indexingSettings.setMongoCredentials(propertiesHolder.sourceMongoUsername,
          propertiesHolder.sourceMongoPassword,
          propertiesHolder.sourceMongoAuthenticationDb);
    }

    if (propertiesHolder.sourceMongoEnableSSL) {
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
    indexingSettings.setZookeeperDefaultCollection(propertiesHolder.destinationZookeeperDefaultCollection);
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
