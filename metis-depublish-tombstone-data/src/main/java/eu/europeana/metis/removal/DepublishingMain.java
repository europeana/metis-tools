package eu.europeana.metis.removal;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.removal.config.Configuration;
import eu.europeana.metis.removal.config.DefaultConfiguration;
import eu.europeana.metis.removal.config.PropertiesHolder;
import eu.europeana.metis.removal.execution.ExecutorManager;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;

/**
 * Entry class for the remove tombstone script.
 *
 */
public class DepublishingMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublishingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws InterruptedException, IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException, IOException {
    LOGGER.info("Starting script");

    final Configuration configuration = new DefaultConfiguration(propertiesHolder);

    boolean startExecution = true;
    if (configuration.isClearDatabasesBeforeProcess()) {
      System.out.println(
          "Script parameter to clear databases before start is set to true, " + "continue? y/n");
      try (Scanner input = new Scanner(System.in)) {
        char c = input.next().charAt(0);
        if (c != 'y') {
          startExecution = false;
        }
      }
    }

    if (startExecution) {
      final ExecutorManager executorManager = new ExecutorManager(configuration,
          propertiesHolder);
      executorManager.startDepublishTombstoneData();
      executorManager.close();
    }
    configuration.close();
    LOGGER.info("End script");
  }
}
