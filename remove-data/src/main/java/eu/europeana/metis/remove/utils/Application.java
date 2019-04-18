package eu.europeana.metis.remove.utils;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.Closeable;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Application implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

  private final PropertiesHolder properties;
  private final MongoInitializer mongoInitializer;
  private final MorphiaDatastoreProvider datastoreProvider;

  private Application(PropertiesHolder properties, MongoInitializer mongoInitializer,
      MorphiaDatastoreProvider datastoreProvider) {
    this.properties = properties;
    this.mongoInitializer = mongoInitializer;
    this.datastoreProvider = datastoreProvider;
  }

  public static Application initialize() throws TrustStoreConfigurationException {

    final PropertiesHolder properties = new PropertiesHolder();

    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(properties.truststorePath) && StringUtils
        .isNotEmpty(properties.truststorePassword)) {
      CustomTruststoreAppender
          .appendCustomTrustoreToDefault(properties.truststorePath, properties.truststorePassword);
    }

    final MongoInitializer mongoInitializer = new MongoInitializer(properties);
    mongoInitializer.initializeMongoClient();
    final MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), properties.mongoDb);

    return new Application(properties, mongoInitializer, morphiaDatastoreProvider);
  }

  @Override
  public void close() {
    mongoInitializer.close();
  }

  public PropertiesHolder getProperties() {
    return properties;
  }

  public MorphiaDatastoreProvider getDatastoreProvider() {
    return datastoreProvider;
  }
}
