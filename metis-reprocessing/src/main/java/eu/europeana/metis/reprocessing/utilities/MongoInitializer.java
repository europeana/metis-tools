package eu.europeana.metis.reprocessing.utilities;

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
 * @since 2019-04-16
 */
public class MongoInitializer {

  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDb;
  private MongoClient mongoClient;

  public MongoInitializer(String[] mongoHosts, int[] mongoPorts, String mongoAuthenticationDb,
      String mongoUsername, String mongoPassword, boolean mongoEnablessl, String mongoDb) {
    this.mongoHosts = mongoHosts;
    this.mongoPorts = mongoPorts;
    this.mongoAuthenticationDb = mongoAuthenticationDb;
    this.mongoUsername = mongoUsername;
    this.mongoPassword = mongoPassword;

    this.mongoEnablessl = mongoEnablessl;
    this.mongoDb = mongoDb;
  }

  public void initializeMongoClient() {
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
    if (StringUtils.isEmpty(mongoDb) || StringUtils
        .isEmpty(mongoUsername) || StringUtils
        .isEmpty(mongoPassword)) {
      mongoClient = new MongoClient(serverAddresses, optionsBuilder.build());
    } else {
      MongoCredential mongoCredential = MongoCredential
          .createCredential(mongoUsername, mongoAuthenticationDb,
              mongoPassword.toCharArray());
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
