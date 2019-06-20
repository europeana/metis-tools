package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.time.Duration;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the {@link ExecutorService} class and handles the parallelization of the tasks.
 * <p>Parallelization of tasks is performed per dataset. So each thread will be processing one
 * dataset at a time. The total amount of allowed parallel threads is calculated according to the
 * total available threads and how many threads a dataset will consume while it's being
 * processed.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String REPROCESSED_DATASETS_STR = "Reprocessed datasets: {}";
  private final BasicConfiguration basicConfiguration;
  private final int maxParallelThreadsPerDataset;
  private final int totalAllowedThreads;
  private final int startFromDatasetIndex;
  private final int endAtDatasetIndex;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(BasicConfiguration basicConfiguration,
      PropertiesHolder propertiesHolder) {
    int maxParallelThreads = propertiesHolder.minParallelDatasets;
    this.maxParallelThreadsPerDataset = propertiesHolder.maxParallelThreadsPerDataset;
    this.startFromDatasetIndex = propertiesHolder.startFromDatasetIndex;
    this.endAtDatasetIndex = propertiesHolder.endAtDatasetIndex;
    totalAllowedThreads = maxParallelThreads * maxParallelThreadsPerDataset;
    threadPool = Executors.newFixedThreadPool(totalAllowedThreads);
    completionService = new ExecutorCompletionService<>(threadPool);

    this.basicConfiguration = basicConfiguration;
  }

  public void startReprocessing() throws InterruptedException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Calculating order of datasets for processing..");
    final List<String> allDatasetIds = basicConfiguration.getMetisCoreMongoDao()
        .getAllDatasetIds();
    final List<Long> sizeOfAllDatasetIds = allDatasetIds.stream()
        .map(datasetId -> basicConfiguration.getMongoSourceMongoDao()
            .getTotalRecordsForDataset(datasetId)).collect(Collectors.toList());
    final List<String> sortedAllDatasetIds = allDatasetIds.stream()
        .sorted(Comparator.comparingLong(o -> sizeOfAllDatasetIds.get(allDatasetIds.indexOf(o)))
            .reversed()).collect(Collectors.toList());
    final List<String> nonZeroSortedAllDatasetIds = sortedAllDatasetIds.stream()
        .filter(datasetId -> sizeOfAllDatasetIds.get(allDatasetIds.indexOf(datasetId)) > 0)
        .collect(Collectors.toList());
//    IntStream.range(0, nonZeroSortedAllDatasetIds.size())
//        .forEach(i -> retrieveOrInitializeDatasetStatus(nonZeroSortedAllDatasetIds.get(i), i));

    Date startDate = new Date();
    Timer timer = new Timer();
    ScheduledThreadForSpeedProjection st = new ScheduledThreadForSpeedProjection(startDate,
        nonZeroSortedAllDatasetIds, sizeOfAllDatasetIds);
    timer.scheduleAtFixedRate(st, Duration.ofMinutes(10).toMillis(),
        Duration.ofMinutes(10).toMillis());

    int parallelDatasets = 0;
    int reprocessedDatasets = 0;

    int countOfTotalCurrentThreads = 0;
    final HashMap<Future, Integer> futureConsumedThreadsHashMap = new HashMap<>();

    for (int i = startFromDatasetIndex;
        i < endAtDatasetIndex && i < nonZeroSortedAllDatasetIds.size(); i++) {
      String datasetId = nonZeroSortedAllDatasetIds.get(i);
      final Long sizeOfDatasetId = sizeOfAllDatasetIds.get(allDatasetIds.indexOf(datasetId));
      final int numberOfPages = (int) Math
          .ceil((double) sizeOfDatasetId / MongoSourceMongoDao.PAGE_SIZE);
      int maxThreadsConsumedByDataset =
          numberOfPages >= maxParallelThreadsPerDataset ? maxParallelThreadsPerDataset
              : numberOfPages;

      while (countOfTotalCurrentThreads >= totalAllowedThreads || maxThreadsConsumedByDataset > (
          totalAllowedThreads - countOfTotalCurrentThreads)) {
        final Future<Void> completedFuture = completionService.take();
        final Integer futureConsumedThreads = futureConsumedThreadsHashMap.get(completedFuture);
        futureConsumedThreadsHashMap.remove(completedFuture);
        parallelDatasets--;
        countOfTotalCurrentThreads -= futureConsumedThreads;
        reprocessedDatasets++;
        LOGGER.info(REPROCESSED_DATASETS_STR, reprocessedDatasets);
        LOGGER.info(EXECUTION_LOGS_MARKER, REPROCESSED_DATASETS_STR, reprocessedDatasets);
      }
      Callable<Void> callable;
      switch (basicConfiguration.getMode()) {
        case DEFAULT:
        case REPROCESS_ALL_FAILED:
        default:
          callable = new ReprocessForDataset(datasetId, i, basicConfiguration,
              maxParallelThreadsPerDataset);
      }
      final Future<Void> submit = completionService.submit(callable);
      futureConsumedThreadsHashMap.put(submit, maxThreadsConsumedByDataset);
      parallelDatasets++;
      countOfTotalCurrentThreads += maxThreadsConsumedByDataset;
    }

    for (int i = 0; i < parallelDatasets; i++) {
      completionService.take();
      reprocessedDatasets++;
      LOGGER.info(EXECUTION_LOGS_MARKER, REPROCESSED_DATASETS_STR, reprocessedDatasets);
    }
    timer.cancel();
  }

//  private DatasetStatus retrieveOrInitializeDatasetStatus(String datasetId,
//      int indexInOrderedList) {
//    DatasetStatus retrievedDatasetStatus = basicConfiguration.getMongoDestinationMongoDao()
//        .getDatasetStatus(datasetId);
//    if (retrievedDatasetStatus == null) {
//      retrievedDatasetStatus = new DatasetStatus();
//      final long totalRecordsForDataset = basicConfiguration.getMongoSourceMongoDao()
//          .getTotalRecordsForDataset(datasetId);
//      retrievedDatasetStatus.setDatasetId(datasetId);
//      retrievedDatasetStatus.setIndexInOrderedList(indexInOrderedList);
//      retrievedDatasetStatus.setTotalRecords(totalRecordsForDataset);
//      basicConfiguration.getMongoDestinationMongoDao()
//          .storeDatasetStatusToDb(retrievedDatasetStatus);
//    }
//    return retrievedDatasetStatus;
//  }

  public void close() {
    threadPool.shutdown();
  }

  /**
   * Interal {@link TimerTask} that is supposed to run as a daemon thread periodically, to calculate
   * the speed and time required for the current full operation to complete.
   */
  private class ScheduledThreadForSpeedProjection extends TimerTask {

    private final Date startDate;
    private final List<String> datasetIds;
    private final List<Long> sizeOfAllDatasetIds;
    private final long totalPreviouslyProcessed;

    ScheduledThreadForSpeedProjection(Date startDate, List<String> datasetIds,
        List<Long> sizeOfAllDatasetIds) {
      this.startDate = startDate;
      this.datasetIds = datasetIds;
      this.sizeOfAllDatasetIds = sizeOfAllDatasetIds;
      this.totalPreviouslyProcessed = datasetIds.stream()
          .map(id -> basicConfiguration.getMongoDestinationMongoDao().getDatasetStatus(id))
          .filter(Objects::nonNull).mapToLong(DatasetStatus::getTotalProcessed).sum();
    }

    public void run() {
      LOGGER.info(EXECUTION_LOGS_MARKER, "Scheduled calculation of projection speed started");
      Date nowDate = new Date();
      long secondsInBetween = (nowDate.getTime() - startDate.getTime()) / 1000;
      //Only calculate projected date if a defined time threshold has passed
      final long totalRecords = sizeOfAllDatasetIds.stream().reduce(0L, Long::sum);
      final List<DatasetStatus> datasetStatusesSnapshot = datasetIds.stream()
          .map(id -> basicConfiguration.getMongoDestinationMongoDao().getDatasetStatus(id))
          .filter(Objects::nonNull).collect(Collectors.toList());
      final long totalProcessedFromStartDate = datasetStatusesSnapshot.stream()
          .filter(ds -> ds.getStartDate() != null)
          .filter(ds -> ds.getStartDate().compareTo(startDate) >= 0)
          .mapToLong(DatasetStatus::getTotalProcessed).sum();

      final double recordsPerSecond =
          (double) (totalProcessedFromStartDate - totalPreviouslyProcessed) / secondsInBetween;
      final double totalHoursRequired =
          (totalRecords / (recordsPerSecond <= 0 ? 1 : recordsPerSecond)) / 3600;
      final double totalHoursRequiredWithoutPreviouslyProcessed =
          ((totalRecords - totalPreviouslyProcessed) / (recordsPerSecond <= 0 ? 1
              : recordsPerSecond)) / 3600;
      final Date projectedEndDate = Date
          .from(startDate.toInstant().plus(
              Duration.ofMinutes((long) (totalHoursRequiredWithoutPreviouslyProcessed * 60))));

      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Average time required, with current speed, for a full reprocess: {} Hours, projected end date: {}",
          totalHoursRequired, projectedEndDate);
    }
  }
}
