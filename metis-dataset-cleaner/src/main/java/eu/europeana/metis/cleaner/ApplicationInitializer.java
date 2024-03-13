package eu.europeana.metis.cleaner;

import eu.europeana.metis.cleaner.common.PropertyFileLoader;
import eu.europeana.metis.cleaner.utilities.IndexWrapper;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationInitializer.class);
  private static final Properties indexingProperties = new Properties();
  private final IndexWrapper indexWrapper;

  public ApplicationInitializer() {
    PropertyFileLoader.loadPropertyFile("",
        "application.properties",
        indexingProperties);
    LOGGER.info("Indexing properties loaded.");
    indexWrapper = IndexWrapper.getInstance(indexingProperties);
  }

  public IndexWrapper getIndexWrapper() {
    return indexWrapper;
  }
}