package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
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

  public ExecutorManager(Datastore metisCoreDatastore, Datastore mongoSourceDatastore,
      Datastore mongoDestinationDatastore) {
    this.mongoDao = new MongoDao(metisCoreDatastore, mongoSourceDatastore, mongoDestinationDatastore);
  }

  public void startReprocessing() {
//    final List<String> allDatasetIds = mongoDao.getAllDatasetIdsOrdered();
    String datasetId = "03915";
    final long startTime = System.nanoTime();
    List<FullBeanImpl> nextPageOfRecords = mongoDao.getNextPageOfRecords(datasetId, 0);
//    List<FullBeanImpl> nextPageOfRecords = new ArrayList<>();
//    for (int i = 0; i < 1000; i++) {
//      nextPageOfRecords.add(mongoDao.getRecord(
//          "/03915/public_mistral_memoire_fr_ACTION_CHERCHER_FIELD_1_REF_VALUE_1_AP70L00682F"));
//    }
    System.out.println(nextPageOfRecords.size());
    final long endTime = System.nanoTime();
    System.out.println("Total time: " + (double) (endTime - startTime) / 1_000_000_000.0);
    final DatasetStatus datasetStatus = new DatasetStatus();
    datasetStatus.setDatasetId(datasetId);
    datasetStatus.setTotalProcessed(200);
    mongoDao.storeDatasetStatusToDb(datasetStatus);
  }

  public void close() {
  }

}
