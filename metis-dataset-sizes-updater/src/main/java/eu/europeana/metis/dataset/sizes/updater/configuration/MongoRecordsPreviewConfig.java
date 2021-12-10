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
public class MongoRecordsPreviewConfig {
  @Value("${mongo.records.preview.hosts}")
  private  String[] mongoHosts;
  @Value("${mongo.records.preview.port}")
  private  int[] mongoPorts;
  @Value("${mongo.records.preview.authentication.db}")
  private  String mongoAuthenticationDb;
  @Value("${mongo.records.preview.username}")
  private  String mongoUsername;
  @Value("${mongo.records.preview.password}")
  public  String mongoPassword;
  @Value("${mongo.records.preview.enableSSL}")
  private  boolean mongoEnableSSL;
  @Value("${mongo.records.preview.db}")
  private  String mongoDb;
  @Value("${mongo.records.preview.application.name}")
  private String mongoApplicationName;

  @Bean("recordPreviewDao")
  public RecordDao getRecordDao(MongoClient mongoRecordsPreviewClient) {
    return new RecordDao(mongoRecordsPreviewClient, mongoDb);
  }

  @Bean("mongoRecordsPreviewClient")
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
