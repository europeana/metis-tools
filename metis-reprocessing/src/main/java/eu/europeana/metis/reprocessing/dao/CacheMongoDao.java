package eu.europeana.metis.reprocessing.dao;

import com.mongodb.MongoClient;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class CacheMongoDao {

  private static final String RESOURCE_URL = "resourceUrl";
  private Datastore mongoCacheDatastore;
  private PropertiesHolderExtension propertiesHolderExtension;
  private final MongoInitializer mongoCacheMongoInitializer;

  public CacheMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolderExtension = propertiesHolder.getPropertiesHolderExtension();
    mongoCacheMongoInitializer = prepareMongoCacheConfiguration();
    mongoCacheDatastore = createMongoCacheDatastore(
        mongoCacheMongoInitializer.getMongoClient(), propertiesHolderExtension.cacheMongoDb);
  }

  public TechnicalMetadataWrapper getTechnicalMetadataWrapper(String resourceUrl) {
    return mongoCacheDatastore.find(TechnicalMetadataWrapper.class)
        .field(RESOURCE_URL).equal(resourceUrl).get();
  }

  private MongoInitializer prepareMongoCacheConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.cacheMongoHosts,
        propertiesHolderExtension.cacheMongoPorts,
        propertiesHolderExtension.cacheMongoAuthenticationDb,
        propertiesHolderExtension.cacheMongoUsername, propertiesHolderExtension.cacheMongoPassword,
        propertiesHolderExtension.cacheMongoEnablessl, propertiesHolderExtension.cacheMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMongoCacheDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(TechnicalMetadataWrapper.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    datastore.ensureIndexes();
    return datastore;
  }

  public void close() {
    mongoCacheMongoInitializer.close();
  }
}
