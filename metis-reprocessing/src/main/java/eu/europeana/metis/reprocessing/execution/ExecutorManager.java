package eu.europeana.metis.reprocessing.execution;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.config.Mode;
import eu.europeana.metis.reprocessing.config.PropertiesHolder;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

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
  private static final String PROCESSED_DATASETS_STR = "Processed datasets: {}";
  private final Configuration configuration;
  private final int maxParallelThreadsPerDataset;
  private final int totalAllowedThreads;
  private final int startFromDatasetIndex;
  private final int endAtDatasetIndex;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(Configuration configuration, PropertiesHolder propertiesHolder) {
    int maxParallelThreads = propertiesHolder.minParallelDatasets;
    this.maxParallelThreadsPerDataset = propertiesHolder.maxParallelThreadsPerDataset;
    this.startFromDatasetIndex = propertiesHolder.startFromDatasetIndex;
    this.endAtDatasetIndex = propertiesHolder.endAtDatasetIndex;
    totalAllowedThreads = maxParallelThreads * maxParallelThreadsPerDataset;
    threadPool = Executors.newFixedThreadPool(totalAllowedThreads);
    completionService = new ExecutorCompletionService<>(threadPool);

    this.configuration = configuration;
  }

  public void startReprocessing() throws InterruptedException {
    LOGGER
        .info("Starting with mode, startIndex, endIndex, " + "totalAllowedThreads: {}, {}, {}, {}",
            configuration.getMode(), startFromDatasetIndex, endAtDatasetIndex,
            totalAllowedThreads);
    //In default mode we try cleanup
    if (configuration.getMode().equals(Mode.DEFAULT)) {
      checkForCleaningDatabases();
    } else if (configuration.getMode().equals(Mode.CLEAN)) {
      checkForCleaningDatabases();
      //We only clean dbs and return
      return;
    } else if (configuration.getMode().equals(Mode.POST_CLEAN)) {
      //We only clean/remove the collections that were created for assisting the processing
      configuration.getMongoDestinationMongoDao().dropTemporaryCollections();
      return;
    }
    final List<DatasetStatus> datasetStatuses = getDatasetStatuses();
    if (CollectionUtils.isEmpty(datasetStatuses)) {
      LOGGER.error("No available dataset statuses");
      return;
    }

    Date startDate = new Date();
    Timer timer = new Timer();
    ScheduledThreadForSpeedProjection st = new ScheduledThreadForSpeedProjection(startDate,
        datasetStatuses);
    timer.scheduleAtFixedRate(st, Duration.ofMinutes(10).toMillis(),
        Duration.ofMinutes(10).toMillis());

    int parallelDatasets = 0;
    int reprocessedDatasets = 0;

    int countOfTotalCurrentThreads = 0;
    final HashMap<Future<Void>, Integer> futureConsumedThreadsHashMap = new HashMap<>();

    for (int i = startFromDatasetIndex; i < endAtDatasetIndex && i < datasetStatuses.size(); i++) {
      final DatasetStatus datasetStatus = datasetStatuses.get(i);
      final int numberOfPages = (int) Math
          .ceil((double) datasetStatus.getTotalRecords() / MongoSourceMongoDao.PAGE_SIZE);
      int maxThreadsConsumedByDataset = Math.min(numberOfPages, maxParallelThreadsPerDataset);

      while (countOfTotalCurrentThreads >= totalAllowedThreads || maxThreadsConsumedByDataset > (
          totalAllowedThreads - countOfTotalCurrentThreads)) {
        final Future<Void> completedFuture = completionService.take();
        try {
          //Check and log for exceptions
          completedFuture.get();
        } catch (ExecutionException e) {
          LOGGER.error("An exception occurred in a callable.", e);
        }
        final Integer futureConsumedThreads = futureConsumedThreadsHashMap.get(completedFuture);
        futureConsumedThreadsHashMap.remove(completedFuture);
        parallelDatasets--;
        countOfTotalCurrentThreads -= futureConsumedThreads;
        reprocessedDatasets++;
        LOGGER.info(PROCESSED_DATASETS_STR, reprocessedDatasets);
        LOGGER.info(PROCESSED_DATASETS_STR, reprocessedDatasets);
      }
      Callable<Void> callable = new ProcessDataset(datasetStatus, configuration,
          maxParallelThreadsPerDataset);
      final Future<Void> submit = completionService.submit(callable);
      futureConsumedThreadsHashMap.put(submit, maxThreadsConsumedByDataset);
      parallelDatasets++;
      countOfTotalCurrentThreads += maxThreadsConsumedByDataset;
    }

    for (int i = 0; i < parallelDatasets; i++) {
      final Future<Void> completedFuture = completionService.take();
      try {
        //Check and log for exceptions
        completedFuture.get();
      } catch (Exception e) {
        LOGGER.error("An exception occurred in a callable.", e);
      }
      reprocessedDatasets++;
      LOGGER.info(PROCESSED_DATASETS_STR, reprocessedDatasets);
    }
    commitSolrChanges();
    timer.cancel();
  }

  private List<DatasetStatus> getDatasetStatuses() {
    LOGGER.info("Calculating order of datasets for processing..");

    final List<DatasetStatus> datasetStatuses;
    if (configuration.getMode().equals(Mode.REPROCESS_ALL_FAILED)) {
      datasetStatuses = configuration.getMongoDestinationMongoDao().getAllDatasetStatuses();
      datasetStatuses.removeIf(datasetStatus -> datasetStatus.getTotalFailedRecords() <= 0);
    } else if (configuration.getMode().equals(Mode.POST_PROCESS)) {
      datasetStatuses = configuration.getMongoDestinationMongoDao().getAllDatasetStatuses();
    } else {
      //We want the biggest datasets to start first
      final Map<String, Long> datasetsWithSize = getDatasetsWithSize();
      AtomicInteger atomicIndex = new AtomicInteger(0);
      datasetStatuses = datasetsWithSize.entrySet().stream().filter(entry -> entry.getValue() > 0)
          .sorted(Collections.reverseOrder(comparingByValue())).map(
              entry -> retrieveOrInitializeDatasetStatus(entry.getKey(),
                  atomicIndex.getAndIncrement(), entry.getValue())).collect(Collectors.toList());
    }
    LOGGER.info("Calculated order of datasets for processing");
    return datasetStatuses;
  }

  public void checkForCleaningDatabases() {
    if (configuration.getMode().equals(Mode.CLEAN) || configuration
        .isClearDatabasesBeforeProcess()) {
      LOGGER.info("Cleaning database");
      configuration.getMongoDestinationMongoDao().deleteAll();
      try {
        configuration.getDestinationCompoundSolrClient().getSolrClient().deleteByQuery("*:*");
        configuration.getDestinationIndexer().triggerFlushOfPendingChanges(true);
      } catch (SolrServerException | IOException | IndexingException e) {
        LOGGER.warn("Could not cleanup solr", e);
      }
      LOGGER.info("Cleaned database");
    }
  }

  private void commitSolrChanges() {
    //Commit changes
    //The indexer shouldn't be closed here, therefore it's not initialized in a
    //try-with-resources block
    try {
      LOGGER.info("Commit changes");
      configuration.getDestinationIndexer().triggerFlushOfPendingChanges(true);
      LOGGER.info("Committed changes");
    } catch (IndexingException e) {
      LOGGER.warn("Could not commit changes to solr, changes will be visible after auto commit", e);
    }
  }

  private Map<String, Long> getDatasetsWithSize() {
    if (configuration.getDatasetIdsToProcess().isEmpty()) {
      return getDatasetWithSizeFromCore();
    } else {
      return getDatasetWithSizeFromProvidedList();
    }
  }

  private Map<String, Long> getDatasetWithSizeFromCore() {
    return configuration.getMetisCoreMongoDao().getAllDatasetIds().parallelStream().collect(
        toMap(Function.identity(), datasetId -> configuration.getMongoSourceMongoDao()
                                                             .getTotalRecordsForDataset(datasetId)));
  }

  private Map<String, Long> getDatasetWithSizeFromProvidedList() {
    return configuration.getDatasetIdsToProcess().stream().collect(toMap(Function.identity(),
        datasetId -> configuration.getMongoSourceMongoDao()
                                  .getTotalRecordsForDataset(datasetId)));
  }

  /**
   * Either get a {@link DatasetStatus} that already exists or generate one.
   *
   * @param indexInOrderedList the index of the dataset in the original ordered list
   * @return the dataset status
   */
  private DatasetStatus retrieveOrInitializeDatasetStatus(String datasetId, int indexInOrderedList,
      long totalRecordsForDataset) {
    DatasetStatus retrievedDatasetStatus = configuration.getMongoDestinationMongoDao()
                                                        .getDatasetStatus(datasetId);
    if (retrievedDatasetStatus == null) {
      retrievedDatasetStatus = new DatasetStatus();
      retrievedDatasetStatus.setDatasetId(datasetId);
      retrievedDatasetStatus.setIndexInOrderedList(indexInOrderedList);
      retrievedDatasetStatus.setTotalRecords(totalRecordsForDataset);
      configuration.getMongoDestinationMongoDao()
                   .storeDatasetStatusToDb(retrievedDatasetStatus);
    }
    return retrievedDatasetStatus;
  }

  public void close() {
    threadPool.shutdown();
  }

  /**
   * Interal {@link TimerTask} that is supposed to run as a daemon thread periodically, to calculate
   * the speed and time required for the current full operation to complete.
   */
  private class ScheduledThreadForSpeedProjection extends TimerTask {

    private final Date startDate;
    private final long totalPreviouslyProcessed;
    private final List<DatasetStatus> datasetStatuses;
    private final long totalRecords;

    ScheduledThreadForSpeedProjection(Date startDate, List<DatasetStatus> datasetStatuses) {
      this.startDate = startDate;
      this.datasetStatuses = datasetStatuses;
      this.totalPreviouslyProcessed = datasetStatuses.stream().map(
          datasetStatus -> configuration.getMongoDestinationMongoDao()
                                        .getDatasetStatus(datasetStatus.getDatasetId())).filter(Objects::nonNull)
          .mapToLong(DatasetStatus::getTotalProcessed).sum();
      this.totalRecords = datasetStatuses.stream().map(DatasetStatus::getTotalRecords)
          .reduce(0L, Long::sum);
    }

    public void run() {
      LOGGER.info("Scheduled calculation of projection speed started");
      Date nowDate = new Date();
      long secondsInBetween = (nowDate.getTime() - startDate.getTime()) / 1000;
      //Only calculate projected date if a defined time threshold has passed
      final List<DatasetStatus> datasetStatusesSnapshot = datasetStatuses.stream().map(
          datasetStatus -> configuration.getMongoDestinationMongoDao()
                                        .getDatasetStatus(datasetStatus.getDatasetId())).filter(Objects::nonNull)
          .collect(toList());
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
      final Date projectedEndDate = Date.from(startDate.toInstant()
          .plus(Duration.ofMinutes((long) (totalHoursRequiredWithoutPreviouslyProcessed * 60))));

      LOGGER.info(String.format(
          "Average time required, with current speed, for a full reprocess: %.2f Hours, projected end date: %s",
          totalHoursRequired, projectedEndDate));
    }
  }
}
