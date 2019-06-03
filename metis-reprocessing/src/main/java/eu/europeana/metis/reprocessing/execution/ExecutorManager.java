package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the {@link ExecutorService} class and handles the parallelization of the tasks.
 * <p>Parallelization of tasks is performed per dataset. So each thread will be processing one
 * dataset at a time.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String REPROCESSED_DATASETS_STR = "Reprocessed datasets: {}";
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
    int reprocessedDatasets = 0;

    for (int i = startFromDatasetIndex; i < endAtDatasetIndex; i++) {
      String datasetId = allDatasetIds.get(i);
      // TODO: 17-5-19 remove the below line
//      datasetId = "0940417";
      Callable<Void> callable;
      switch (basicConfiguration.getMode()) {
        case CALCULATE_DATASET_STATISTICS:
          callable = new StatisticsForDataset(datasetId, basicConfiguration, false);
          break;
        case CALCULATE_DATASET_STATISTICS_SAMPLE:
          callable = new StatisticsForDataset(datasetId, basicConfiguration, true);
          break;
        case DEFAULT:
        case REPROCESS_ALL_FAILED:
        default:
          callable = new ReprocessForDataset(datasetId, i, basicConfiguration);
      }
      if (threadCounter >= maxParallelThreads) {
        completionService.take();
        threadCounter--;
        reprocessedDatasets++;
        LOGGER.info(REPROCESSED_DATASETS_STR, reprocessedDatasets);
        LOGGER.info(EXECUTION_LOGS_MARKER, REPROCESSED_DATASETS_STR, reprocessedDatasets);
      }
      completionService.submit(callable);
      threadCounter++;
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
      reprocessedDatasets++;
      LOGGER.info(EXECUTION_LOGS_MARKER, REPROCESSED_DATASETS_STR, reprocessedDatasets);
    }
  }

  public void close() {
    threadPool.shutdown();
  }

}
