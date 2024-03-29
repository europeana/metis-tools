package eu.europeana.metis.execution.utilities;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.mongo.connection.MongoProperties.ReadPreferenceValue;

/**
 * Initialize MongoClient
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class MongoInitializer {

  private final String[] mongoHosts;
  private final int[] mongoPorts;
  private final String mongoAuthenticationDb;
  private final String mongoUsername;
  private final String mongoPassword;
  private final boolean mongoEnablessl;
  private final String mongoApplicationName;
  private MongoClient mongoClient;

  public MongoInitializer(String[] mongoHosts, int[] mongoPorts, String mongoAuthenticationDb,
      String mongoUsername, String mongoPassword, boolean mongoEnablessl, String mongoApplicationName) {
    this.mongoHosts = mongoHosts;
    this.mongoPorts = mongoPorts;
    this.mongoAuthenticationDb = mongoAuthenticationDb;
    this.mongoUsername = mongoUsername;
    this.mongoPassword = mongoPassword;

    this.mongoEnablessl = mongoEnablessl;
    this.mongoApplicationName = mongoApplicationName;
  }

  public void initializeMongoClient() {
    mongoClient = new MongoClientProvider<>(getMongoProperties()).createMongoClient();
  }

  private MongoProperties<IllegalArgumentException> getMongoProperties() {
    final MongoProperties<IllegalArgumentException> mongoProperties = new MongoProperties<>(
        IllegalArgumentException::new);
    mongoProperties.setAllProperties(mongoHosts, mongoPorts, mongoAuthenticationDb, mongoUsername,
        mongoPassword, mongoEnablessl, ReadPreferenceValue.SECONDARY, mongoApplicationName);
    return mongoProperties;
  }

  public void close() {
    mongoClient.close();
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }
}
