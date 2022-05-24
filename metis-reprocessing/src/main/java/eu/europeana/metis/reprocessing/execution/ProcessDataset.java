package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import eu.europeana.metis.reprocessing.model.Mode;
import eu.europeana.metis.schema.jibx.RDF;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a {@link Callable} class that would be initialized with a {@link #datasetId}.
 * <p>It is responsible of re-processing a dataset as a whole, by paging records from the
 * database, while keeping track of it's dataset status. This class should not require modification
 * and only provided functionality in the {@link BasicConfiguration} should be modifiable.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ProcessDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessDataset.class);
  private final String prefixDatasetIdLog;
  private final String datasetId;
  private final DatasetStatus datasetStatus;
  private final BasicConfiguration basicConfiguration;
  private final int maxParallelPageThreads;
  private int nextPage;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Integer> completionService;

  ProcessDataset(DatasetStatus datasetStatus, BasicConfiguration basicConfiguration,
      int maxParallelPageThreads) {
    this.datasetId = datasetStatus.getDatasetId();
    this.basicConfiguration = basicConfiguration;
    this.prefixDatasetIdLog = String.format("DatasetId: %s", this.datasetId);
    this.datasetStatus = datasetStatus;

    this.maxParallelPageThreads = maxParallelPageThreads;
    threadPool = Executors.newFixedThreadPool(maxParallelPageThreads);
    completionService = new ExecutorCompletionService<>(threadPool);
  }

  @Override
  public Void call() throws ExecutionException, InterruptedException {
    processDataset();
    return null;
  }

  private void processDataset() throws ExecutionException, InterruptedException {
    LOGGER.info("{} - Processing start", prefixDatasetIdLog);
    final long startProcessTime = System.nanoTime();
    if (basicConfiguration.getMode() == Mode.DEFAULT) {
      if (datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()) {
        LOGGER.info(
            "{} - Reprocessing not started because it was already completely processed with totalRecords: {} - totalProcessed: {}",
            prefixDatasetIdLog, datasetStatus.getTotalRecords(), datasetStatus.getTotalProcessed());
        return;
      }
      //Process normally if not completely processed
      loopOverAllRecordsAndProcess();
      finalizeDatasetStatus(startProcessTime);
    } else if (basicConfiguration.getMode() == Mode.REPROCESS_ALL_FAILED) {
      if (datasetStatus.getTotalFailedRecords() <= 0) {
        //Do not process dataset further cause we only process failed ones
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "{} - Reprocessing not started because mode is: {} and there are no failed records",
              prefixDatasetIdLog, basicConfiguration.getMode().name());
        }
        return;
      }
      LOGGER.info(
          "{} - Reprocessing will happen only on previously failed records, number of which is {}",
          prefixDatasetIdLog, datasetStatus.getTotalFailedRecords());
      //Process only failed records no matter if the dataset has already been completed
      loopOverAllFailedRecordsAndProcess();
      finalizeDatasetStatus(startProcessTime);
    } else if (basicConfiguration.getMode() == Mode.POST_PROCESS) {
      postProcess();
      LOGGER.info("{} - Applied post processing function", prefixDatasetIdLog);
    }
    LOGGER.info("{} - Processing end", prefixDatasetIdLog);
    close();
  }

  private void finalizeDatasetStatus(long startProcess) {
    final double elapsedTime = nanoTimeToSeconds(System.nanoTime() - startProcess);
    datasetStatus
        .setActualTimeProcessAndIndex(datasetStatus.getActualTimeProcessAndIndex() + elapsedTime);
    basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    LOGGER.info("{} - DatasetStatus - {}", prefixDatasetIdLog, datasetStatus);
    LOGGER
        .info(STATISTICS_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetIdLog, datasetStatus);
  }

  /**
   * Retrieve the starting next page index based on a {@link DatasetStatus}.
   *
   * @return the index of the next page
   */
  private int getStartingNextPage() {
    final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
    int startingPage;
    if (pagesProcessed.isEmpty()) {
      startingPage = 0;
    } else {
      startingPage = IntStream.range(0, pagesProcessed.size())
          .filter(idx -> !pagesProcessed.contains(idx)).findFirst().orElse(pagesProcessed.size());
    }
    return startingPage;
  }

  private void loopOverAllRecordsAndProcess() throws ExecutionException, InterruptedException {
    nextPage = getStartingNextPage();
    datasetStatus.setStartDate(new Date());
    basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    defaultOperation();
  }

  private void loopOverAllFailedRecordsAndProcess() {
    failedRecordsOperation();
  }

  private void failedRecordsOperation() {
    //For an execution of processing only previously failed records the starting page index is
    // always 0(zero). And since we update individual records nextPage remains at 0 on each iteration.
    int nextPage = 0;
    final long totalFailedRecords = datasetStatus.getTotalFailedRecords();
    List<FailedRecord> failedRecords = getFailedRecords(nextPage);
    List<FullBeanImpl> nextPageOfRecords = getFailedFullBeans(failedRecords);
    long counterFailedRecordsProcessed = 0;
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetIdLog,
          nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        final String exceptionStackTrace = processAndIndex(fullBean);
        failedRecords.stream()
            .filter(failedRecord -> failedRecord.getFailedUrl().equals(fullBean.getAbout()))
            .findFirst().ifPresent(
            foundFailedRecord -> updateProcessFailedOnlyCounts(exceptionStackTrace,
                fullBean.getAbout(), foundFailedRecord));
      }
      counterFailedRecordsProcessed += nextPageOfRecords.size();
      LOGGER.info("{} - Processed number of records: {} out of total number of failed records: {}",
          prefixDatasetIdLog, counterFailedRecordsProcessed, totalFailedRecords);
      failedRecords = getFailedRecords(nextPage);
      nextPageOfRecords = getFailedFullBeans(failedRecords);
    }
    //    basicConfiguration.getMongoDestinationMongoDao().deleteAllSuccessfulReprocessedFailedRecords();
  }

  /**
   * Default processing operation.
   * <p>It calculates all sorts of statistics provided in the {@link DatasetStatus} in the
   * datastore. Creates new {@link PageProcess} classes and starts them under the thread pool, while
   * finalizing the operation by running {@link #postProcess()} when all records have been
   * processed.
   */
  private void defaultOperation() throws InterruptedException, ExecutionException {
    LOGGER
        .info("{} - Already processed: {}", prefixDatasetIdLog, datasetStatus.getTotalProcessed());
    int threadCounter = 0;
    while (true) {
      Callable<Integer> callable = new PageProcess(this, datasetId);
      //Create callable that returns the number of records processed from the page
      if (threadCounter >= maxParallelPageThreads) {
        final Future<Integer> recordsInPageProcessed = completionService.take();
        threadCounter--;
        //If page here was less than the page size, then exit while
        if (recordsInPageProcessed.get() < MongoSourceMongoDao.PAGE_SIZE) {
          break;
        }
      }
      completionService.submit(callable);
      threadCounter++;
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
    }

    //Set End Date
    datasetStatus.setEndDate(new Date());
    basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
  }

  void processRecords(List<FullBeanImpl> nextPageOfRecords) {
    for (FullBeanImpl fullBean : nextPageOfRecords) {
      final String exceptionStackTrace = processAndIndex(fullBean);
      updateProcessCounts(exceptionStackTrace, fullBean.getAbout());
    }
  }

  void updateDatasetStatus(int pageProcessed) {
    synchronized (this) {
      final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
      pagesProcessed.add(pageProcessed);
      datasetStatus.updateAverages();
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    }
  }

  int getNextPageAndIncrement() {
    synchronized (this) {
      final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
      int nextPageToReturn;
      if (pagesProcessed.size() <= nextPage) {
        nextPageToReturn = nextPage;
        nextPage++;
      } else {
        nextPageToReturn = IntStream.range(nextPage, pagesProcessed.size())
            .filter(idx -> !pagesProcessed.contains(idx)).findFirst().orElse(pagesProcessed.size());
        nextPage = nextPageToReturn + 1;
      }
      return nextPageToReturn;
    }
  }

  /**
   * Get the {@link FullBeanImpl}s representation from the failed records provided.
   *
   * @param failedRecords the failed records
   * @return the list of records
   */
  private List<FullBeanImpl> getFailedFullBeans(List<FailedRecord> failedRecords) {
    final List<String> failedRecordsUrls = failedRecords.stream().map(FailedRecord::getFailedUrl)
        .collect(Collectors.toList());
    return basicConfiguration.getMongoSourceMongoDao().getRecordsFromList(failedRecordsUrls);
  }

  private List<FailedRecord> getFailedRecords(int nextPage) {
    return basicConfiguration.getMongoDestinationMongoDao()
        .getNextPageOfFailedRecords(datasetId, nextPage);
  }

  /**
   * Get the next page of {@link FullBeanImpl}s from the source database.
   *
   * @param nextPage the next page of records
   * @return the list of records
   */
  List<FullBeanImpl> getFullBeans(int nextPage) {
    return basicConfiguration.getMongoSourceMongoDao().getNextPageOfRecords(datasetId, nextPage);
  }

  private String processAndIndex(FullBeanImpl fullBean) {
    try {
      final RDF rdf = processRecord(fullBean);
      indexRecord(rdf);
    } catch (ProcessingException e) {
      LOGGER.error("{} - Could not process record: {}", prefixDatasetIdLog, fullBean.getAbout(), e);
      return exceptionStacktraceToString(e);
    } catch (IndexingException e) {
      LOGGER.error("{} - Could not index record: {}", prefixDatasetIdLog, fullBean.getAbout(), e);
      return exceptionStacktraceToString(e);
    }
    return "";
  }

  private void updateProcessFailedOnlyCounts(String exceptionStackTrace, String resourceId,
      FailedRecord foundFailedRecord) {
    updateProcessCounts(exceptionStackTrace, true, resourceId, foundFailedRecord);
  }

  private void updateProcessCounts(String exceptionStackTrace, String resourceId) {
    updateProcessCounts(exceptionStackTrace, false, resourceId, null);
  }

  /**
   * Update the record counts based on the execution.
   * <p>For a failed record it will create a new {@link FailedRecord}.
   * For a success record if the {@code processFailedOnly} is true then the {@link FailedRecord} is
   * removed and the {@link DatasetStatus#getTotalFailedRecords()} is updated.</p>
   *
   * @param exceptionStackTrace the exception stack trace if any
   * @param processFailedOnly trigger for execution of processing only previously failed records
   * @param resourceId the processed record identifier
   */
  private void updateProcessCounts(String exceptionStackTrace, boolean processFailedOnly,
      String resourceId, FailedRecord foundFailedRecord) {
    synchronized (this) {
      if (StringUtils.isNotBlank(resourceId)) {
        if (StringUtils.isBlank(exceptionStackTrace)) {
          if (processFailedOnly) {
            basicConfiguration.getMongoDestinationMongoDao()
                .deleteFailedRecordFromDb(foundFailedRecord);
            datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() - 1);
            basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
          } else {
            datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
          }
        } else {
          final FailedRecord failedRecord = new FailedRecord(resourceId, exceptionStackTrace);
          if (foundFailedRecord != null) {
            failedRecord.setId(foundFailedRecord.getId());
          }
          basicConfiguration.getMongoDestinationMongoDao().storeFailedRecordToDb(failedRecord);
          if (!processFailedOnly) {
            datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
            datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
          }
        }
      }
    }
  }

  private RDF processRecord(FullBeanImpl fullBean) throws ProcessingException {
    final long startTimeProcess = System.nanoTime();
    try {
      return basicConfiguration.getFullBeanProcessor().apply(fullBean, basicConfiguration);
    } finally {
      final double elapsedTime = nanoTimeToSeconds(System.nanoTime() - startTimeProcess);
      synchronized (this) {
        datasetStatus.setTotalTimeProcessingInSecs(
            datasetStatus.getTotalTimeProcessingInSecs() + elapsedTime);
      }
    }
  }

  private void indexRecord(RDF rdf) throws IndexingException {
    final long startTimeIndex = System.nanoTime();
    try {
      basicConfiguration.getRdfIndexer().accept(rdf, true, basicConfiguration);
    } finally {
      final double elapsedTime = nanoTimeToSeconds(System.nanoTime() - startTimeIndex);
      synchronized (this) {
        datasetStatus
            .setTotalTimeIndexingInSecs(datasetStatus.getTotalTimeIndexingInSecs() + elapsedTime);
      }
    }
  }

  private void postProcess() {
    try {
      basicConfiguration.getAfterReprocessProcessor()
          .accept(datasetId, datasetStatus.getStartDate(), datasetStatus.getEndDate(),
              basicConfiguration);
    } catch (ProcessingException e) {
      LOGGER.error("{} - After reprocessing operation failed!", prefixDatasetIdLog, e);
    }
  }

  private double nanoTimeToSeconds(long nanoTime) {
    return nanoTime / 1_000_000_000.0;
  }

  private static String exceptionStacktraceToString(Exception e) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    e.printStackTrace(ps);
    ps.close();
    return baos.toString();
  }

  public void close() {
    threadPool.shutdown();
  }
}
