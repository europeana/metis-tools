package eu.europeana.metis.remove.utils;

import com.mongodb.MongoClient;
import eu.europeana.metis.mongo.MongoClientProvider;

/**
 * Initialize MongoClient
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class MongoInitializer {

  private final PropertiesHolder propertiesHolder;
  private MongoClient mongoClient;

  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  public void initializeMongoClient() {
    this.mongoClient = new MongoClientProvider<>(propertiesHolder.getMongoCoreProperties())
            .createMongoClient();
  }

  public void close() {
    mongoClient.close();
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }
}
