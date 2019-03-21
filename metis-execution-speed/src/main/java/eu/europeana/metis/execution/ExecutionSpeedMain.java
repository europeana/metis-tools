package eu.europeana.metis.execution;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.execution.utilities.AverageMaintainer;
import eu.europeana.metis.execution.utilities.ExecutorManager;
import eu.europeana.metis.execution.utilities.MongoInitializer;
import eu.europeana.metis.execution.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class ExecutionSpeedMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionSpeedMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args) throws TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting execution speed script");
    final MongoInitializer mongoInitializer = prepareConfiguration();
    final MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    final ExecutorManager executorManager = new ExecutorManager(morphiaDatastoreProvider);
    for (PluginType pluginType : PluginType.values()) {
      final AverageMaintainer averageMaintainer = executorManager
          .startCalculation(pluginType, 10, 0);
      LOGGER.info("PluginType: {} - Total Samples: {}, Total Average: {}", pluginType,
          averageMaintainer.getTotalSamples(), averageMaintainer.getTotalAverageInSecs());
    }

    mongoInitializer.close();

  }

  public static MongoInitializer prepareConfiguration() throws TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

}
