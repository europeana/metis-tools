package eu.europeana.metis.mongo.analyzer.utilities;

import eu.europeana.metis.mongo.MongoProperties;
import eu.europeana.metis.mongo.analyzer.model.Mode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-01
 */
@Configuration
//@formatter:off
@PropertySources({
    @PropertySource(value = "file:application.properties", ignoreResourceNotFound = true),
    @PropertySource("classpath:application.properties")
})
//@formatter:on
public class ConfigurationPropertiesHolder {

  @Value("${truststore.path}")
  private String truststorePath;
  @Value("${truststore.password}")
  private String truststorePassword;
  @Value("${socks.proxy.enabled}")
  private boolean socksProxyEnabled = false;
  @Value("${socks.proxy.host}")
  private String socksProxyHost;
  @Value("${socks.proxy.port}")
  private String socksProxyPort;
  @Value("${socks.proxy.username}")
  private String socksProxyUsername;
  @Value("${socks.proxy.password}")
  private String socksProxyPassword;
  @Value("${mongo.hosts}")
  private String[] mongoHosts;
  @Value("${mongo.port}")
  private int[] mongoPorts;
  @Value("${mongo.authentication.db}")
  private String mongoAuthenticationDb;
  @Value("${mongo.username}")
  private String mongoUsername;
  @Value("${mongo.password}")
  private String mongoPassword;
  @Value("${mongo.enableSSL}")
  private boolean mongoEnableSSL = false;
  @Value("${mongo.db}")
  private String mongoDb;
  @Value("${log.counter.checkpoint}")
  private long logCounterCheckpoint;

  @Value("${mode}")
  private Mode mode;
  @Value("${record.about.to.check}")
  private String recordAboutToCheck;

  @Value("${file.path.with.corrupted.records}")
  private String filePathWithCorruptedRecords;

  public MongoProperties<IllegalArgumentException> getMongoProperties() {
    final MongoProperties<IllegalArgumentException> mongoProperties = new MongoProperties<>(
        IllegalArgumentException::new);
    mongoProperties.setAllProperties(mongoHosts, mongoPorts, mongoAuthenticationDb, mongoUsername,
        mongoPassword, mongoEnableSSL, null);
    return mongoProperties;
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

  public long getLogCounterCheckpoint() {
    return logCounterCheckpoint;
  }

  public Mode getMode() {
    return mode;
  }

  public String getRecordAboutToCheck() {
    return recordAboutToCheck;
  }

  public String getFilePathWithCorruptedRecords() {
    return filePathWithCorruptedRecords;
  }
}
