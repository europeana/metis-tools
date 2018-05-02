package eu.europeana.metis.endpoints.mapper;

import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.endpoints.mapper.utilities.ExecutorManager;
import eu.europeana.metis.endpoints.mapper.utilities.MongoInitializer;
import eu.europeana.metis.endpoints.mapper.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Main method that starts the script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class EnpointsMapperMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnpointsMapperMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args) throws TrustStoreConfigurationException {
    PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting migration script");
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    DatasetDao datasetDaoOriginal = new DatasetDao(
        new MorphiaDatastoreProvider(mongoInitializer.getMongoClient(),
            propertiesHolder.mongoDbOriginal), null);
    DatasetDao datasetDaoTemporary = new DatasetDao(
        new MorphiaDatastoreProvider(mongoInitializer.getMongoClient(),
            propertiesHolder.mongoDbTemporary), null);
    ExecutorManager executorManager = new ExecutorManager(propertiesHolder, datasetDaoOriginal,
        datasetDaoTemporary);

    switch (propertiesHolder.mode) {
      case CREATE_MAP:
        LOGGER
            .info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Mode {}", propertiesHolder.mode.name());
        executorManager.createMode();
        break;
      case REVERSE_MAP:
        LOGGER
            .info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Mode {}", propertiesHolder.mode.name());
        executorManager.deleteMode();
        break;
      default:
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Mode not supported.");
        break;
    }

    mongoInitializer.close();
  }
}
