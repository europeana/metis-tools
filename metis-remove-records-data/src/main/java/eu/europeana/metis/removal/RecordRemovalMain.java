package eu.europeana.metis.removal;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.removal.config.Configuration;
import eu.europeana.metis.removal.config.DefaultConfiguration;
import eu.europeana.metis.removal.config.PropertiesHolder;
import eu.europeana.metis.removal.execution.ExecutorManager;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Record removal main.
 */
public class RecordRemovalMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordRemovalMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws InterruptedException the interrupted exception
   * @throws IndexingException the indexing exception
   * @throws URISyntaxException the uri syntax exception
   * @throws TrustStoreConfigurationException the trust store configuration exception
   * @throws IOException the io exception
   */
  public static void main(String[] args)
      throws InterruptedException, IndexingException, URISyntaxException,
      TrustStoreConfigurationException, IOException {
    LOGGER.info("Starting script");

    try (Configuration configuration = new DefaultConfiguration(propertiesHolder);
        ExecutorManager executorManager = new ExecutorManager(configuration, propertiesHolder)) {
      executorManager.startRecordsRemovalData();
    }
    LOGGER.info("End script");
  }
}
