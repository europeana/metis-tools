package eu.europeana.metis.execution.utilities;

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
 * @since 2019-03-21
 */
public class MongoInitializer {

  private final PropertiesHolder propertiesHolder;
  private MongoClient mongoClient;

  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  public void initializeMongoClient() {
    if (propertiesHolder.mongoHosts.length != propertiesHolder.mongoPorts.length
        && propertiesHolder.mongoPorts.length != 1) {
      throw new IllegalArgumentException("Mongo hosts and ports are not properly configured.");
    }

    List<ServerAddress> serverAddresses = new ArrayList<>();
    for (int i = 0; i < propertiesHolder.mongoHosts.length; i++) {
      ServerAddress address;
      if (propertiesHolder.mongoHosts.length == propertiesHolder.mongoPorts.length) {
        address = new ServerAddress(propertiesHolder.mongoHosts[i], propertiesHolder.mongoPorts[i]);
      } else { // Same port for all
        address = new ServerAddress(propertiesHolder.mongoHosts[i], propertiesHolder.mongoPorts[0]);
      }
      serverAddresses.add(address);
    }

    Builder optionsBuilder = new Builder();
    optionsBuilder.sslEnabled(propertiesHolder.mongoEnablessl);
    if (StringUtils.isEmpty(propertiesHolder.mongoDb) || StringUtils
        .isEmpty(propertiesHolder.mongoUsername) || StringUtils
        .isEmpty(propertiesHolder.mongoPassword)) {
      mongoClient = new MongoClient(serverAddresses, optionsBuilder.build());
    } else {
      MongoCredential mongoCredential = MongoCredential
          .createCredential(propertiesHolder.mongoUsername, propertiesHolder.mongoAuthenticationDb,
              propertiesHolder.mongoPassword.toCharArray());
      mongoClient = new MongoClient(serverAddresses, mongoCredential, optionsBuilder.build());
    }
  }

  public void close() {
    mongoClient.close();
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }
}
