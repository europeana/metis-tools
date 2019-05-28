package eu.europeana.metis.remove.utils;

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
 * @since 2018-05-02
 */
public class MongoInitializer {

  private final PropertiesHolder propertiesHolder;
  private MongoClient mongoClient;

  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  public void initializeMongoClient() {
    if (propertiesHolder.mongoCoreHosts.length != propertiesHolder.mongoCorePorts.length
        && propertiesHolder.mongoCorePorts.length != 1) {
      throw new IllegalArgumentException("Mongo hosts and ports are not properly configured.");
    }

    List<ServerAddress> serverAddresses = new ArrayList<>();
    for (int i = 0; i < propertiesHolder.mongoCoreHosts.length; i++) {
      ServerAddress address;
      if (propertiesHolder.mongoCoreHosts.length == propertiesHolder.mongoCorePorts.length) {
        address = new ServerAddress(propertiesHolder.mongoCoreHosts[i], propertiesHolder.mongoCorePorts[i]);
      } else { // Same port for all
        address = new ServerAddress(propertiesHolder.mongoCoreHosts[i], propertiesHolder.mongoCorePorts[0]);
      }
      serverAddresses.add(address);
    }

    Builder optionsBuilder = new Builder();
    optionsBuilder.sslEnabled(propertiesHolder.mongoCoreEnablessl);
    if (StringUtils.isEmpty(propertiesHolder.mongoCoreDb) || StringUtils
        .isEmpty(propertiesHolder.mongoCoreUsername) || StringUtils
        .isEmpty(propertiesHolder.mongoCorePassword)) {
      mongoClient = new MongoClient(serverAddresses, optionsBuilder.build());
    } else {
      MongoCredential mongoCredential = MongoCredential
          .createCredential(propertiesHolder.mongoCoreUsername, propertiesHolder.mongoCoreAuthenticationDb,
              propertiesHolder.mongoCorePassword.toCharArray());
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
