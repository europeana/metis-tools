package eu.europeana.metis.redirects.utilities;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-02-25
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  public ExecutorManager(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
  }
}
