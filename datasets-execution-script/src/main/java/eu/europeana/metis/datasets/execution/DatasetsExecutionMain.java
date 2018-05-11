package eu.europeana.metis.datasets.execution;

import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.datasets.execution.utilities.ExecutorManager;
import eu.europeana.metis.datasets.execution.utilities.MongoInitializer;
import eu.europeana.metis.datasets.execution.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-08
 */
public class DatasetsExecutionMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetsExecutionMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main( String[] args ) throws TrustStoreConfigurationException {
    PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting datasets execution script");
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
    DatasetDao datasetDaoOriginal = new DatasetDao(morphiaDatastoreProvider, null);
    ExecutorManager executorManager = new ExecutorManager(propertiesHolder, datasetDaoOriginal);
    executorManager.startExecutions();

    mongoInitializer.close();
  }

}
