package eu.europeana.metis.execution;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.exception.BadContentException;
import eu.europeana.metis.execution.utilities.ExecutorManager;
import eu.europeana.metis.execution.utilities.MongoInitializer;
import eu.europeana.metis.execution.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class ExecutionSpeedMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionSpeedMain.class);
  private static final String CONFIGURATION_FILE = "application-prod.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws BadContentException, CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting execution speed script");
    if (propertiesHolder.endNumberOfDaysAgo < 0
        || propertiesHolder.startNumberOfDaysAgo < propertiesHolder.endNumberOfDaysAgo) {
      throw new BadContentException(
          "Both startNumberOfDaysAgo and endNumberOfDaysAgo must be a positive number and startNumberOfDaysAgo has to be a higher or equal number than endNumberOfDaysAgo");
    }

    final MongoInitializer mongoInitializer = prepareConfiguration();
    final MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    final ExecutorManager executorManager = new ExecutorManager(morphiaDatastoreProvider);
    executorManager.startCalculationForAllPluginTypes(propertiesHolder.startNumberOfDaysAgo,
        propertiesHolder.endNumberOfDaysAgo);

    mongoInitializer.close();

  }

  private static MongoInitializer prepareConfiguration()
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.mongoHosts,
        propertiesHolder.mongoPorts, propertiesHolder.mongoAuthenticationDb,
        propertiesHolder.mongoUsername, propertiesHolder.mongoPassword,
        propertiesHolder.mongoEnablessl);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

}
