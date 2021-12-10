package eu.europeana.metis.dataset.sizes.updater.configuration;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.mongo.connection.MongoProperties.ReadPreferenceValue;
import eu.europeana.metis.mongo.dao.RecordDao;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Initialize MongoClient
 */
@Configuration
public class MongoRecordsPublishConfig {
  @Value("${mongo.records.publish.hosts}")
  private  String[] mongoHosts;
  @Value("${mongo.records.publish.port}")
  private  int[] mongoPorts;
  @Value("${mongo.records.publish.authentication.db}")
  private  String mongoAuthenticationDb;
  @Value("${mongo.records.publish.username}")
  private  String mongoUsername;
  @Value("${mongo.records.publish.password}")
  public  String mongoPassword;
  @Value("${mongo.records.publish.enableSSL}")
  private  boolean mongoEnableSSL;
  @Value("${mongo.records.publish.db}")
  private  String mongoDb;
  @Value("${mongo.records.publish.application.name}")
  private String mongoApplicationName;

  @Bean("recordPublishDao")
  public RecordDao getRecordPublishDao(MongoClient mongoRecordsPublishClient) {
    return new RecordDao(mongoRecordsPublishClient, mongoDb);
  }

  @Bean("mongoRecordsPublishClient")
  public MongoClient getMongoClient() {
    return new MongoClientProvider<>(getMongoProperties()).createMongoClient();
  }

  private MongoProperties<IllegalArgumentException> getMongoProperties() {
    final MongoProperties<IllegalArgumentException> mongoProperties = new MongoProperties<>(
        IllegalArgumentException::new);
    mongoProperties.setAllProperties(mongoHosts, mongoPorts, mongoAuthenticationDb, mongoUsername,
        mongoPassword, mongoEnableSSL, ReadPreferenceValue.PRIMARY, mongoApplicationName);
    return mongoProperties;
  }
}
