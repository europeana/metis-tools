package eu.europeana.metis.technical.metadata.generation;

import com.mongodb.MongoClient;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.utilities.ExecutorManager;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoInitializer;
import eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class TechnicalMetadataGenerationMain {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TechnicalMetadataGenerationMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws TrustStoreConfigurationException, IOException, MediaProcessorException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting script");

    final MongoInitializer mongoInitializer = prepareConfiguration();
    final Datastore datastore = createDatastore(mongoInitializer.getMongoClient(),
        propertiesHolder.mongoDb);

    final ExecutorManager executorManager = new ExecutorManager(datastore,
        propertiesHolder.directoryWithResourcesPerDatasetPath,
        propertiesHolder.startFromBeginningOfFiles, propertiesHolder.retryFailedResources);
    executorManager.startTechnicalMetadataGeneration();
    executorManager.close();
    mongoInitializer.close();

  }

  private static MongoInitializer prepareConfiguration() throws TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Append default truststore with custom truststore");
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createDatastore(MongoClient mongoClient, String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(TechnicalMetadataWrapper.class);
    morphia.map(FileStatus.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    datastore.ensureIndexes();
    return datastore;
  }

}
