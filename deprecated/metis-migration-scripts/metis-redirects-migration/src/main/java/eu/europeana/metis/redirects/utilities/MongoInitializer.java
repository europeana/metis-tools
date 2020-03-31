package eu.europeana.metis.redirects.utilities;

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
 * @since 2020-02-25
 */
public class MongoInitializer {

  private final PropertiesHolder propertiesHolder;
  private MongoClient redirectsMongoClient;
  private MongoClient recordsMongoClient;

  public MongoInitializer(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  public void initializeRedirectsMongoClient() {
    redirectsMongoClient = initializeMongoClient(propertiesHolder.mongoHosts,
        propertiesHolder.mongoPorts,
        propertiesHolder.mongoEnablessl, propertiesHolder.mongoDb, propertiesHolder.mongoUsername,
        propertiesHolder.mongoPassword, propertiesHolder.mongoAuthenticationDb);
  }

  public void initializeRecordsMongoClient() {
    recordsMongoClient = initializeMongoClient(propertiesHolder.recordsMongoHosts,
        propertiesHolder.recordsMongoPorts, propertiesHolder.recordsMongoEnablessl,
        propertiesHolder.recordsMongoDb, propertiesHolder.recordsMongoUsername,
        propertiesHolder.recordsMongoPassword, propertiesHolder.recordsMongoAuthenticationDb);
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
    if (recordsMongoClient != null) {
      redirectsMongoClient.close();
    }
    if (recordsMongoClient != null) {
      recordsMongoClient.close();
    }
  }

  public MongoClient getRedirectsMongoClient() {
    return redirectsMongoClient;
  }

  public MongoClient getRecordsMongoClient() {
    return recordsMongoClient;
  }
}
