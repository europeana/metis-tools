package eu.europeana.metis.creator;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.creator.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.mongo.MongoClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initialize MongoClient
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
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
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.getTruststorePath(),
          propertiesHolder.getTruststorePassword());
    }

    // Initialize the socks proxy.
    if (propertiesHolder.isSocksProxyEnabled()) {
      System.setProperty("socksProxyHost", propertiesHolder.getSocksProxyHost());
      System.setProperty("socksProxyPort", propertiesHolder.getSocksProxyPort());
      Authenticator.setDefault(new Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(propertiesHolder.getSocksProxyUsername(),
              propertiesHolder.getSocksProxyPassword().toCharArray());
        }
      });
    }

    // Initialize the Mongo connection
    return new MongoClientProvider<>(propertiesHolder.getMongoProperties()).createMongoClient();
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }
}
