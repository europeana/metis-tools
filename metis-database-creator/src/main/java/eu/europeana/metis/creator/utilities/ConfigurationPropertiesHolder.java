package eu.europeana.metis.creator.utilities;

import eu.europeana.metis.mongo.MongoProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
@Component
@PropertySource({"classpath:application.properties"})
public class ConfigurationPropertiesHolder {

  @Value("${creation.database.type}")
  public  CreationDatabaseType creationDatabaseType;
  @Value("${truststore.path}")
  public  String truststorePath;
  @Value("${truststore.password}")
  public  String truststorePassword;
  @Value("${socks.proxy.enabled}")
  private boolean socksProxyEnabled;
  @Value("${socks.proxy.host}")
  private String socksProxyHost;
  @Value("${socks.proxy.port}")
  private String socksProxyPort;
  @Value("${socks.proxy.username}")
  private String socksProxyUsername;
  @Value("${socks.proxy.password}")
  private String socksProxyPassword;
  @Value("${mongo.hosts}")
  public  String[] mongoHosts;
  @Value("${mongo.port}")
  public  int[] mongoPorts;
  @Value("${mongo.authentication.db}")
  public  String mongoAuthenticationDb;
  @Value("${mongo.username}")
  public  String mongoUsername;
  @Value("${mongo.password}")
  public  String mongoPassword;
  @Value("${mongo.enableSSL}")
  public  boolean mongoEnableSSL;
  @Value("${mongo.db}")
  public  String mongoDb;

  public MongoProperties<IllegalArgumentException> getMongoProperties() {
    final MongoProperties<IllegalArgumentException> mongoProperties = new MongoProperties<>(
        IllegalArgumentException::new);
    mongoProperties.setAllProperties(mongoHosts, mongoPorts, mongoAuthenticationDb, mongoUsername,
        mongoPassword, mongoEnableSSL, null);
    return mongoProperties;
  }

  public CreationDatabaseType getCreationDatabaseType() {
    return creationDatabaseType;
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public boolean isSocksProxyEnabled() {
    return socksProxyEnabled;
  }

  public String getSocksProxyHost() {
    return socksProxyHost;
  }

  public String getSocksProxyPort() {
    return socksProxyPort;
  }

  public String getSocksProxyUsername() {
    return socksProxyUsername;
  }

  public String getSocksProxyPassword() {
    return socksProxyPassword;
  }

  public String[] getMongoHosts() {
    return mongoHosts;
  }

  public int[] getMongoPorts() {
    return mongoPorts;
  }

  public String getMongoAuthenticationDb() {
    return mongoAuthenticationDb;
  }

  public String getMongoUsername() {
    return mongoUsername;
  }

  public String getMongoPassword() {
    return mongoPassword;
  }

  public boolean isMongoEnableSSL() {
    return mongoEnableSSL;
  }

  public String getMongoDb() {
    return mongoDb;
  }
}
