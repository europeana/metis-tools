package eu.europeana.metis.creator.utilities;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Initialize MongoClient
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
public class MongoInitializer {

  private final PropertiesHolder propertiesHolder;
  private MongoClient mongoClient;

  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  public void initializeMongoClient() {
    mongoClient = initializeMongoClient(propertiesHolder.mongoHosts,
        propertiesHolder.mongoPorts, propertiesHolder.mongoEnablessl,
        propertiesHolder.mongoDb, propertiesHolder.mongoUsername,
        propertiesHolder.mongoPassword, propertiesHolder.mongoAuthenticationDb);
  }

  private MongoClient initializeMongoClient(String[] mongoHosts, int[] mongoPorts,
      boolean mongoEnablessl, String mongoDb, String mongoUsername, String mongoPassword,
      String mongoAuthenticationDb) {
    if (mongoHosts.length != mongoPorts.length
        && mongoPorts.length != 1) {
      throw new IllegalArgumentException("Mongo hosts and ports are not properly configured.");
    }

    List<ServerAddress> serverAddresses = new ArrayList<>();
    for (int i = 0; i < mongoHosts.length; i++) {
      ServerAddress address;
      if (mongoHosts.length == mongoPorts.length) {
        address = new ServerAddress(mongoHosts[i], mongoPorts[i]);
      } else { // Same port for all
        address = new ServerAddress(mongoHosts[i], mongoPorts[0]);
      }
      serverAddresses.add(address);
    }

    Builder optionsBuilder = new Builder();
    optionsBuilder.sslEnabled(mongoEnablessl);
    if (StringUtils.isEmpty(mongoDb) || StringUtils.isEmpty(mongoUsername) || StringUtils
        .isEmpty(mongoPassword)) {
      return new MongoClient(serverAddresses, optionsBuilder.build());
    } else {
      MongoCredential mongoCredential = MongoCredential
          .createCredential(mongoUsername, mongoAuthenticationDb, mongoPassword.toCharArray());
      return new MongoClient(serverAddresses, mongoCredential, optionsBuilder.build());
    }
  }

  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }
}
