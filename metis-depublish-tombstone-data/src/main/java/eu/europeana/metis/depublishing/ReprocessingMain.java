package eu.europeana.metis.depublishing;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.depublishing.config.Configuration;
import eu.europeana.metis.depublishing.config.DefaultConfiguration;
import eu.europeana.metis.depublishing.config.PropertiesHolder;
import eu.europeana.metis.depublishing.execution.ExecutorManager;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry class for the reprocessing script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessingMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws InterruptedException, IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException, IOException {
    LOGGER.info("Starting script");

    final Configuration configuration = new DefaultConfiguration(propertiesHolder);

    final ExecutorManager executorManager = new ExecutorManager(configuration,
        propertiesHolder);
    executorManager.startReprocessing();
    executorManager.close();

    configuration.close();
    LOGGER.info("End script");
  }
}
