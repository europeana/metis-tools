package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private static final String PROCESSED_DATASETS_STR = "Processed datasets: {}";
  private final BasicConfiguration basicConfiguration;
  private final int maxParallelThreads;
  private final int startFromDatasetIndex;
  private final int endAtDatasetIndex;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(BasicConfiguration basicConfiguration,
      PropertiesHolder propertiesHolder) {
    this.maxParallelThreads = propertiesHolder.maxParallelThreads;
    this.startFromDatasetIndex = propertiesHolder.startFromDatasetIndex;
    this.endAtDatasetIndex = propertiesHolder.endAtDatasetIndex;
    threadPool = Executors.newFixedThreadPool(maxParallelThreads);
    completionService = new ExecutorCompletionService<>(threadPool);

    this.basicConfiguration = basicConfiguration;
  }

  public void startReprocessing() throws InterruptedException {
    final List<String> allDatasetIds = basicConfiguration.getMetisCoreMongoDao()
        .getAllDatasetIdsOrdered();
    int threadCounter = 0;
    int processedDatasets = 0;

    for (int i = startFromDatasetIndex; i <= endAtDatasetIndex; i++) {
      String datasetId = allDatasetIds.get(i);
      // TODO: 17-5-19 remove the below line
      datasetId = "2048716";
      final ReprocessForDataset reprocessForDataset = new ReprocessForDataset(datasetId,
          basicConfiguration);
      if (threadCounter >= maxParallelThreads) {
        completionService.take();
        threadCounter--;
        processedDatasets++;
        LOGGER.info(PROCESSED_DATASETS_STR, processedDatasets);
        LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_DATASETS_STR, processedDatasets);
      }
      completionService.submit(reprocessForDataset);
      threadCounter++;
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
      processedDatasets++;
      LOGGER.info(PROCESSED_DATASETS_STR, processedDatasets);
      LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_DATASETS_STR, processedDatasets);
    }
  }

  public void close() {
    threadPool.shutdown();
  }

}
