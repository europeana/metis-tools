package eu.europeana.metis.reprocessing;

import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.execution.ExecutorManager;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.ExtraConfiguration;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;
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
      throws InterruptedException, IndexingException, URISyntaxException, CustomTruststoreAppender.TrustStoreConfigurationException, IOException, DereferenceException, EnrichmentException {
    LOGGER.info("Starting script");

    final BasicConfiguration basicConfiguration = new BasicConfiguration(propertiesHolder);
    final ExtraConfiguration extraConfiguration = new ExtraConfiguration(propertiesHolder);
    basicConfiguration.setExtraConfiguration(extraConfiguration);

    boolean startExecution = true;
    if (basicConfiguration.isClearDatabasesBeforeProcess()) {
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
      final ExecutorManager executorManager = new ExecutorManager(basicConfiguration,
          propertiesHolder);
      executorManager.startReprocessing();
      executorManager.close();
    }
    basicConfiguration.close();
    LOGGER.info("End script");
  }
}
