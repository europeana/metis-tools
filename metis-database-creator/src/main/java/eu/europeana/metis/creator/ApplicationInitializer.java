package eu.europeana.metis.creator;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.creator.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initialize MongoClient
 */
public class ApplicationInitializer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationInitializer.class);
  private final MongoClient mongoClient;

  public ApplicationInitializer(ConfigurationPropertiesHolder configurationPropertiesHolder)
      throws TrustStoreConfigurationException {
    mongoClient = initializeApplication(configurationPropertiesHolder);
  }

  private MongoClient initializeApplication(ConfigurationPropertiesHolder propertiesHolder)
      throws TrustStoreConfigurationException {

    // Load the trust store file.
    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.getTruststorePath()) && StringUtils
        .isNotEmpty(propertiesHolder.getTruststorePassword())) {
      CustomTruststoreAppender.appendCustomTruststoreToDefault(propertiesHolder.getTruststorePath(),
          propertiesHolder.getTruststorePassword());
    }

    // Initialize the Mongo connection
    return new MongoClientProvider<>(propertiesHolder.getMongoProperties()).createMongoClient();
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  @Override
  public void close() throws Exception {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }
}
