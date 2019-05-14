package eu.europeana.metis.reprocessing.utilities;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the {@link ExecutorService} class and handles the parallelization of the tasks.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private final MongoDao mongoDao;

  public ExecutorManager(Datastore datastore, PropertiesHolder propertiesHolder) {
    this.mongoDao = new MongoDao(datastore);
  }

  public void startReprocessing(){
    final List<String> allDatasetIds = mongoDao.getAllDatasetIdsOrdered();
  }

  public void close() {
  }

}
