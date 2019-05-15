package eu.europeana.metis.reprocessing.execution;

import eu.europeana.metis.reprocessing.utilities.MongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private static final String PROCESSED_FILES_STR = "Processed datasets: {}";
  private final MongoDao mongoDao;
  private final int maxParallelThreads;
  private final int startFromDatasetIndex;
  private final int endAtDataseteIndex;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(Datastore metisCoreDatastore, Datastore mongoSourceDatastore,
      Datastore mongoDestinationDatastore, PropertiesHolder propertiesHolder) {
    this.maxParallelThreads = propertiesHolder.maxParallelThreads;
    this.startFromDatasetIndex = propertiesHolder.startFromDatasetIndex;
    this.endAtDataseteIndex = propertiesHolder.endAtDatasetIndex;
    this.mongoDao = new MongoDao(metisCoreDatastore, mongoSourceDatastore,
        mongoDestinationDatastore);

    threadPool = Executors.newFixedThreadPool(maxParallelThreads);
    completionService = new ExecutorCompletionService<>(threadPool);
  }

  public void startReprocessing() throws InterruptedException {
    final List<String> allDatasetIds = mongoDao.getAllDatasetIdsOrdered();
    int threadCounter = 0;
    int processedDatasets = 0;
    int datasetIndex = 0;
    for (String datasetId : allDatasetIds) {
      datasetIndex++;
      if (datasetIndex < startFromDatasetIndex) {
        continue;
      }
      if (datasetIndex > endAtDataseteIndex) {
        break;
      }
      final ReprocessForDataset reprocessForDataset = new ReprocessForDataset(datasetId, mongoDao);
      if (threadCounter >= maxParallelThreads) {
        completionService.take();
        threadCounter--;
        processedDatasets++;
        LOGGER.info(PROCESSED_FILES_STR, processedDatasets);
//        LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
      }
      completionService.submit(reprocessForDataset);
      threadCounter++;
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
      processedDatasets++;
      LOGGER.info(PROCESSED_FILES_STR, processedDatasets);
//      LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
    }

//    String datasetId = "03915";
//    final long startTime = System.nanoTime();
//    List<FullBeanImpl> nextPageOfRecords = mongoDao.getNextPageOfRecords(datasetId, 0);
//    List<FullBeanImpl> nextPageOfRecords = new ArrayList<>();
//    for (int i = 0; i < 1000; i++) {
//      nextPageOfRecords.add(mongoDao.getRecord(
//          "/03915/public_mistral_memoire_fr_ACTION_CHERCHER_FIELD_1_REF_VALUE_1_AP70L00682F"));
//    }
//    System.out.println(nextPageOfRecords.size());
//    final long endTime = System.nanoTime();
//    System.out.println("Total time: " + (double) (endTime - startTime) / 1_000_000_000.0);
//    final DatasetStatus datasetStatus = new DatasetStatus();
//    datasetStatus.setDatasetId(datasetId);
//    datasetStatus.setTotalProcessed(200);
//    mongoDao.storeDatasetStatusToDb(datasetStatus);
  }

  public void close() {
    threadPool.shutdown();
  }

}
