package eu.europeana.metis.migration.counter.config;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.mongo.connection.MongoProperties.ReadPreferenceValue;


/**
 * The type Mongo initializer.
 */
public class MongoInitializer {

  private final String[] mongoHosts;
  private final int[] mongoPorts;
  private final String mongoAuthenticationDb;
  private final String mongoUsername;
  private final String mongoPassword;
  private final boolean mongoEnableSSL;
  private final int metisCoreConnectionPoolSize;
  private final String mongoReadPreference;

  private MongoClient mongoClient;

  /**
   * Instantiates a new Mongo initializer.
   *
   * @param propertiesHolder the properties holder
   */
  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.mongoHosts = propertiesHolder.getMetisCoreMongoHosts();
    this.mongoPorts = propertiesHolder.getMetisCoreMongoPorts();
    this.mongoAuthenticationDb = propertiesHolder.getMetisCoreMongoAuthenticationDb();
    this.mongoUsername = propertiesHolder.getMetisCoreMongoUsername();
    this.mongoPassword = propertiesHolder.getMetisCoreMongoPassword();
    this.mongoEnableSSL = propertiesHolder.isMetisCoreMongoEnableSSL();
    this.metisCoreConnectionPoolSize = propertiesHolder.getMetisCoreConnectionPoolSize();
    this.mongoReadPreference = propertiesHolder.getMetisCoreMongoReadPreference();
  }

  /**
   * Initialize mongo client.
   */
  public void initializeMongoClient() {
    mongoClient = new MongoClientProvider<>(getMongoProperties()).createMongoClient();

  }

  private MongoProperties<IllegalArgumentException> getMongoProperties() {
    final MongoProperties<IllegalArgumentException> mongoProperties = new MongoProperties<>(
        IllegalArgumentException::new);
    mongoProperties.setAllProperties(mongoHosts, mongoPorts, mongoAuthenticationDb, mongoUsername,
        mongoPassword, mongoEnableSSL, null, "Metis Reprocessing");
    mongoProperties.setMaxConnectionPoolSize(metisCoreConnectionPoolSize);
    mongoProperties.setReadPreferenceValue(ReadPreferenceValue.valueOf(mongoReadPreference));
    return mongoProperties;
  }

  /**
   * Close.
   */
  public void close() {
    mongoClient.close();
  }

  /**
   * Gets mongo client.
   *
   * @return the mongo client
   */
  public MongoClient getMongoClient() {
    return mongoClient;
  }
}
