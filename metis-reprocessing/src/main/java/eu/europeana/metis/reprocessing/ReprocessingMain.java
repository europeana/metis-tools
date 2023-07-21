package eu.europeana.metis.reprocessing;

import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.config.DefaultConfiguration;
import eu.europeana.metis.reprocessing.config.PropertiesHolderExtension;
import eu.europeana.metis.reprocessing.execution.ExecutorManager;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;

/**
 * Entry class for the reprocessing script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessingMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolderExtension propertiesHolder = new PropertiesHolderExtension(CONFIGURATION_FILE);

  public static void main(String[] args)
          throws InterruptedException, IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException, IOException, DereferenceException, EnrichmentException, NormalizationConfigurationException {
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
      executorManager.startReprocessing();
      executorManager.close();
    }
    configuration.close();
    LOGGER.info("End script");
  }
}
