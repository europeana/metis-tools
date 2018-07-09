package eu.europeana.metis.endpoints.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.endpoints.mapper.utilities.ExecutorManager;
import eu.europeana.metis.endpoints.mapper.utilities.ExtendedWorkflowDao;
import eu.europeana.metis.endpoints.mapper.utilities.Mode;
import eu.europeana.metis.endpoints.mapper.utilities.MongoInitializer;
import eu.europeana.metis.endpoints.mapper.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;

/**
 * The Main method that starts the script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class EndpointsMapperMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(EndpointsMapperMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args) throws TrustStoreConfigurationException {
    PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
    if (propertiesHolder.mode == Mode.NOT_VALID_MODE) {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Mode not supported");
      return;
    }

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
    MorphiaDatastoreProvider morphiaDatastoreProviderOriginal = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDbOriginal);
    MorphiaDatastoreProvider morphiaDatastoreProviderTemporary = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDbTemporary);
    DatasetDao datasetDaoOriginal = new DatasetDao(morphiaDatastoreProviderOriginal, null);
    ExtendedWorkflowDao workflowDaoOriginal = new ExtendedWorkflowDao(morphiaDatastoreProviderOriginal);
    ExtendedWorkflowDao workflowDaoTemporary = new ExtendedWorkflowDao(morphiaDatastoreProviderTemporary);
    ExecutorManager executorManager = new ExecutorManager(propertiesHolder, datasetDaoOriginal, workflowDaoOriginal, workflowDaoTemporary);

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Mode {}", propertiesHolder.mode.name());
    switch (propertiesHolder.mode) {
      case COPY_WORKFLOWS:
        executorManager.copyWorkflowsMode();
        break;
      case CREATE_OAIPMH_WORKFLOWS:
      case CREATE_PREVIEW_WORKFLOWS:
      case CREATE_PUBLISH_WORKFLOWS:
        executorManager.createWorkflows(propertiesHolder.mode);
        break;
      case REVERSE_WORKFLOWS:
        executorManager.reverseWorkflows();
        break;
      default:
        break;
    }
    mongoInitializer.close();
  }
}
