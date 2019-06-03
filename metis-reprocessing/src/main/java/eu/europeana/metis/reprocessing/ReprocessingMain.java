package eu.europeana.metis.reprocessing;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.execution.ExecutorManager;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.ExtraConfiguration;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.net.URISyntaxException;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry class for the reprocessing script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessingMain {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ReprocessingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(
      CONFIGURATION_FILE);

  public static void main(String[] args)
      throws TrustStoreConfigurationException, InterruptedException, IndexingException, URISyntaxException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Starting script");

    final BasicConfiguration basicConfiguration = new BasicConfiguration(propertiesHolder);
    final ExtraConfiguration extraConfiguration = new ExtraConfiguration(propertiesHolder);
    basicConfiguration.setExtraConfiguration(extraConfiguration);

    final ExecutorManager executorManager = new ExecutorManager(basicConfiguration,
        propertiesHolder);
    executorManager.startReprocessing();
    executorManager.close();
    basicConfiguration.close();
    LOGGER.info(EXECUTION_LOGS_MARKER, "End script");
  }


}
