package eu.europeana.metis.reprocessing;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.reprocessing.execution.ExecutorManager;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.ExtraConfiguration;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessingMain {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ReprocessingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolderExtension propertiesHolderExtension = new PropertiesHolderExtension(
      CONFIGURATION_FILE);

  public static void main(String[] args)
      throws TrustStoreConfigurationException, InterruptedException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Starting script");

    final BasicConfiguration basicConfiguration = new BasicConfiguration(propertiesHolderExtension);
    final ExtraConfiguration extraConfiguration = new ExtraConfiguration(propertiesHolderExtension);
    basicConfiguration.setExtraConfiguration(extraConfiguration);

    final ExecutorManager executorManager = new ExecutorManager(basicConfiguration,
        propertiesHolderExtension);
    executorManager.startReprocessing();
    executorManager.close();
    LOGGER.info(EXECUTION_LOGS_MARKER, "End script");
  }

}
