package eu.europeana.metis.tombstone.config;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "app.mode", havingValue = "DEFAULT")
@EnableConfigurationProperties({MongoConfiguration.class, SolrConfiguration.class, ZookeeperConfiguration.class})
public class MetisConfiguration {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final MongoConfiguration mongoConfiguration;
  private final SolrConfiguration solrConfiguration;
  private final ZookeeperConfiguration zookeeperConfiguration;

  @Autowired
  public MetisConfiguration(MongoConfiguration mongoConfiguration,
      SolrConfiguration solrConfiguration,
      ZookeeperConfiguration zookeeperConfiguration) {
    this.mongoConfiguration = mongoConfiguration;
    this.solrConfiguration = solrConfiguration;
    this.zookeeperConfiguration = zookeeperConfiguration;
  }

  @Bean
  public IndexerFactory indexerFactory() throws IndexingException, URISyntaxException {
    IndexingSettings indexingSettings = new IndexingSettings();
    prepareMongoSettings(indexingSettings);
    prepareSolrSettings(indexingSettings);
    prepareZookeeperSettings(indexingSettings);

    return IndexerFactory.create(indexingSettings);
  }

  @Bean
  public Indexer indexer(IndexerFactory indexerFactory) throws SetupRelatedIndexingException {
    return indexerFactory.getIndexer();
  }

  @Bean
  public IndexerPool indexerPool(IndexerFactory indexerFactory) {
    return new IndexerPool(indexerFactory, 600, 60);
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
    for (int i = 0; i < mongoConfiguration.hosts().length; i++) {
      if (mongoConfiguration.hosts().length == mongoConfiguration.ports().length) {
        indexingSettings.addMongoHost(
            new InetSocketAddress(mongoConfiguration.hosts()[i], mongoConfiguration.ports()[i]));
      } else { // Same port for all
        indexingSettings.addMongoHost(
            new InetSocketAddress(mongoConfiguration.hosts()[i], mongoConfiguration.ports()[0]));
      }
    }
    indexingSettings.setMongoDatabaseName(mongoConfiguration.tombstoneDB()/*.sourceMongoTombstoneDb*/);
    indexingSettings.setMongoTombstoneDatabaseName(mongoConfiguration.tombstoneDB());
    if (StringUtils.isEmpty(mongoConfiguration.authenthicationDB()) ||
        StringUtils.isEmpty(mongoConfiguration.userName()) ||
        StringUtils.isEmpty(mongoConfiguration.password()) ||
        StringUtils.isEmpty(mongoConfiguration.tombstoneDB())) {
      log.info("Mongo credentials not provided");
    } else {
      indexingSettings.setMongoCredentials(mongoConfiguration.userName(),
          mongoConfiguration.password(),
          mongoConfiguration.authenthicationDB());
    }

    if (mongoConfiguration.enableSSL()) {
      indexingSettings.setMongoEnableSsl();
    }
  }

  private void prepareSolrSettings(IndexingSettings indexingSettings)
      throws URISyntaxException, SetupRelatedIndexingException {
    for (String instance : solrConfiguration.hosts()) {
      indexingSettings
          .addSolrHost(new URI(instance + zookeeperConfiguration.defaultCollection()));
    }
  }

  private void prepareZookeeperSettings(IndexingSettings indexingSettings)
      throws SetupRelatedIndexingException {
    for (int i = 0; i < zookeeperConfiguration.hosts().length; i++) {
      if (zookeeperConfiguration.hosts().length == zookeeperConfiguration.ports().length) {
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(zookeeperConfiguration.hosts()[i], zookeeperConfiguration.ports()[i]));
      } else { // Same port for all
        indexingSettings.addZookeeperHost(
            new InetSocketAddress(zookeeperConfiguration.hosts()[i], zookeeperConfiguration.ports()[0]));
      }
    }
    indexingSettings.setZookeeperChroot(zookeeperConfiguration.chroot());
    indexingSettings.setZookeeperDefaultCollection(zookeeperConfiguration.defaultCollection());
  }
}
