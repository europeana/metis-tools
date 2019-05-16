package eu.europeana.metis.reprocessing.utilities;

import com.mongodb.MongoClient;
import eu.europeana.metis.reprocessing.model.TechnicalMetadataWrapper;
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

  public CacheMongoDao(PropertiesHolderExtension propertiesHolderExtension) {
    this.propertiesHolderExtension = propertiesHolderExtension;
    mongoCacheMongoInitializer = prepareMongoCacheConfiguration();
    mongoCacheDatastore = createMongoCacheDatastore(
        mongoCacheMongoInitializer.getMongoClient(), propertiesHolderExtension.cacheMongoDb);
  }

  public TechnicalMetadataWrapper getTechnicalMetadataWrapper(String resourceUrl) {
    return mongoCacheDatastore.find(TechnicalMetadataWrapper.class)
        .filter(RESOURCE_URL, resourceUrl).get();
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
    return morphia.createDatastore(mongoClient, databaseName);
  }

  public void close() {
    mongoCacheMongoInitializer.close();
  }
}
