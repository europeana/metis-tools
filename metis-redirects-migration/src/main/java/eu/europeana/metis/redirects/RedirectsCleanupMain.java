package eu.europeana.metis.redirects;

import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.mongo.RecordRedirectDao;
import eu.europeana.metis.redirects.utilities.ExecutorManagerRecordsCleanup;
import eu.europeana.metis.redirects.utilities.MongoInitializer;
import eu.europeana.metis.redirects.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-03
 */
public class RedirectsCleanupMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedirectsCleanupMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  /**
   *
   */
  public static void main(String[] args)
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting redirects cleanup");
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }

    final MongoInitializer mongoInitializer = prepareConfiguration();
    final EdmMongoServerImpl edmMongoServer = new EdmMongoServerImpl(
        mongoInitializer.getRecordsMongoClient(), propertiesHolder.recordsMongoDb, false);
    final RecordRedirectDao recordRedirectDao = new RecordRedirectDao(
        mongoInitializer.getRedirectsMongoClient(), propertiesHolder.mongoDbRedirects, true);

    final ExecutorManagerRecordsCleanup executorManagerRecordsCleanup = new ExecutorManagerRecordsCleanup(
        edmMongoServer, recordRedirectDao, propertiesHolder.rowsPerRequest, propertiesHolder.datasetIdsToKeep);
    executorManagerRecordsCleanup.cleanupDatabaseRedirects();
    executorManagerRecordsCleanup.displayCollectedResults();

    mongoInitializer.close();

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Finished redirects cleanup");
  }

  private static MongoInitializer prepareConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeRedirectsMongoClient();
    mongoInitializer.initializeRecordsMongoClient();
    return mongoInitializer;
  }


}
