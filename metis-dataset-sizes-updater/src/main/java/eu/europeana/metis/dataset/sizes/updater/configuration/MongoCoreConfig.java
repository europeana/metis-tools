package eu.europeana.metis.dataset.sizes.updater.configuration;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.mongo.connection.MongoProperties.ReadPreferenceValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Initialize MongoClient
 */
@Configuration
public class MongoCoreConfig {

  @Value("${mongo.core.hosts}")
  private String[] mongoHosts;
  @Value("${mongo.core.port}")
  private int[] mongoPorts;
  @Value("${mongo.core.authentication.db}")
  private String mongoAuthenticationDb;
  @Value("${mongo.core.username}")
  private String mongoUsername;
  @Value("${mongo.core.password}")
  public String mongoPassword;
  @Value("${mongo.core.enableSSL}")
  private boolean mongoEnableSSL;
  @Value("${mongo.core.db}")
  private String mongoDb;
  @Value("${mongo.core.application.name}")
  private String mongoApplicationName;

  @Bean
  public WorkflowExecutionDao getWorkflowExecutionDao(MorphiaDatastoreProviderImpl morphiaDatastoreProvider) {
    return new WorkflowExecutionDao(morphiaDatastoreProvider);
  }

  @Bean
  public MorphiaDatastoreProviderImpl getMorphiaDatastoreProviderImpl(MongoClient mongoCoreClient) {
    return new MorphiaDatastoreProviderImpl(mongoCoreClient, mongoDb);
  }

  @Bean("mongoCoreClient")
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
