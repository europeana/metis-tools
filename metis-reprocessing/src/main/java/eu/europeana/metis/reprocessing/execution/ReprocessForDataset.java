package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a {@link Callable} class that would be initialized with a {@link #datasetId}.
 * <p>It is responsible of re-processing a dataset as a whole, by paging records from the
 * database, while keeping track of it's dataset status. This class should not require modification
 * and only provided functionality in the {@link BasicConfiguration#getExtraConfiguration()} should
 * be modifiable.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessForDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessForDataset.class);
  private final String prefixDatasetidLog;
  private final String datasetId;
  private final DatasetStatus datasetStatus;
  private final BasicConfiguration basicConfiguration;
  private final int maxParallelPageThreads;
  private int nextPage;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Integer> completionService;

  ReprocessForDataset(DatasetStatus datasetStatus,
      BasicConfiguration basicConfiguration, int maxParallelPageThreads) {
    this.datasetId = datasetStatus.getDatasetId();
    this.basicConfiguration = basicConfiguration;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
    this.datasetStatus = datasetStatus;

    this.maxParallelPageThreads = maxParallelPageThreads;
    threadPool = Executors.newFixedThreadPool(maxParallelPageThreads);
    completionService = new ExecutorCompletionService<>(threadPool);
  }

  @Override
  public Void call() throws ExecutionException, InterruptedException {
    return reprocessDataset();
  }

  private Void reprocessDataset() throws ExecutionException, InterruptedException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Reprocessing starting", prefixDatasetidLog);

    if (basicConfiguration.getMode() == Mode.DEFAULT) {
      if (datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()) {
        LOGGER.info(EXECUTION_LOGS_MARKER,
            "{} - Reprocessing not started because it was already completely processed with totalRecords: {} - totalProcessed: {}",
            prefixDatasetidLog, datasetStatus.getTotalRecords(), datasetStatus.getTotalProcessed());
        return null;
      }
      //Process normally if not completely processed
      loopOverAllRecordsAndProcess(false);
    } else if (basicConfiguration.getMode() == Mode.REPROCESS_ALL_FAILED) {
      if (datasetStatus.getTotalFailedRecords() <= 0) {
        //Do not process dataset further cause we only process failed ones
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(EXECUTION_LOGS_MARKER,
              "{} - Reprocessing not started because mode is: {} and there are no failed records",
              prefixDatasetidLog, basicConfiguration.getMode().name());
        }
        return null;
      }
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "{} - Reprocessing will happen only on previously failed records, number of which is {}",
          prefixDatasetidLog, datasetStatus.getTotalFailedRecords());
      //Process only failed records no matter if the dataset has already been completed
      loopOverAllRecordsAndProcess(true);
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Reprocessing end", prefixDatasetidLog);
    LOGGER
        .info(EXECUTION_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetidLog, datasetStatus);
    LOGGER.info(STATISTICS_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetidLog,
        datasetStatus);
    close();
    return null;
  }

  /**
   * Retrieve the starting next page index based on a {@link DatasetStatus}.
   * <p>For an execution of processing only previously failed records the starting page index is
   * always 0(zero).</p>
   *
   * @param processFailedOnly trigger for execution of processing only previously failed records
   * @return the index of the next page
   */
  private int getStartingNextPage(boolean processFailedOnly) {
    if (processFailedOnly) {
      //Always start from the beginning
      return 0;
    } else {
      final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
      int startingPage;
      if (pagesProcessed.isEmpty()) {
        startingPage = 0;
      } else {
        startingPage = IntStream.range(0, pagesProcessed.size())
            .filter(idx -> !pagesProcessed.contains(idx)).findFirst().orElse(pagesProcessed.size());

        //This can possibly happen if the PAGE_SIZE is changed on a subsequent execution
        //It should be valid even if there are missing pages if the page size is the same
        final boolean isPagingValid =
            MongoSourceMongoDao.PAGE_SIZE * pagesProcessed.size() <= datasetStatus
                .getTotalProcessed() && datasetStatus.getTotalProcessed() / pagesProcessed.size()
                <= (MongoSourceMongoDao.PAGE_SIZE * 2) - 1;

        if (!isPagingValid) {
          startingPage = (int) (datasetStatus.getTotalProcessed() / MongoSourceMongoDao.PAGE_SIZE);
          //Fix processed pages.
          pagesProcessed.clear();
          if (startingPage != 0) {
            IntStream.range(0, startingPage).forEach(pagesProcessed::add);
          }
        }
        //Fix totalProcessed
        datasetStatus
            .setTotalProcessed((long) MongoSourceMongoDao.PAGE_SIZE * pagesProcessed.size());
        basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      }
      return startingPage;
    }
  }

  private void loopOverAllRecordsAndProcess(boolean processFailedOnly)
      throws ExecutionException, InterruptedException {
    nextPage = getStartingNextPage(processFailedOnly);
    if (!processFailedOnly) {
      datasetStatus.setStartDate(new Date());
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    }
    if (processFailedOnly) {
      failedRecordsOperation(nextPage);
    } else {
      defaultOperation();
    }
  }

  private void failedRecordsOperation(int nextPage) {
    final long totalFailedRecords = datasetStatus.getTotalFailedRecords();
    List<FullBeanImpl> nextPageOfRecords = getFailedFullBeans(nextPage);
    long counterFailedRecordsProcessed = 0;
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Processing number of records: {}",
          prefixDatasetidLog, nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        final String exceptionStackTrace = processAndIndex(fullBean);
        updateProcessFailedOnlyCounts(exceptionStackTrace, fullBean.getAbout());
      }
      counterFailedRecordsProcessed += nextPageOfRecords.size();
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "{} - Processed number of records: {} out of total number of failed records: {}",
          prefixDatasetidLog, counterFailedRecordsProcessed, totalFailedRecords);
      nextPage++;
      nextPageOfRecords = getFailedFullBeans(nextPage);
    }
    basicConfiguration.getMongoDestinationMongoDao().deleteAllSuccessfulReprocessedFailedRecords();
  }

  /**
   * Default processing operation.
   * <p>It calculates all sorts of statistics provided in the {@link DatasetStatus} in the
   * datastore. Creates new {@link PageProcess} classes and starts them under the thread pool, while
   * finalizing the operation by running {@link #afterReProcess()} when all records have been
   * processed.
   */
  private void defaultOperation() throws InterruptedException, ExecutionException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Already processed: {}", prefixDatasetidLog,
        datasetStatus.getTotalProcessed());
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
    afterReProcess();
    LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Applied after reprocessing function",
        prefixDatasetidLog);
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

  private List<FullBeanImpl> getFailedFullBeans(int nextPage) {
    return getFullBeans(nextPage, true);
  }

  List<FullBeanImpl> getFullBeans(int nextPage) {
    return getFullBeans(nextPage, false);
  }

  /**
   * Retrieved the {@link FullBeanImpl}s.
   * <p>For {@code processFailedOnly} true, the {@link FailedRecord}s datastore is checked instead
   * of the source datastore.</p>
   *
   * @param nextPage the next page of records
   * @param processFailedOnly trigger for execution of processing only previously failed records
   * @return the list of records
   */
  private List<FullBeanImpl> getFullBeans(int nextPage, boolean processFailedOnly) {
    if (processFailedOnly) {
      final List<FailedRecord> nextPageOfFailedRecords = basicConfiguration
          .getMongoDestinationMongoDao().getNextPageOfFailedRecords(datasetId, nextPage);
      final List<String> failedRecordsUrls = nextPageOfFailedRecords.stream()
          .map(FailedRecord::getFailedUrl)
          .collect(Collectors.toList());
      return basicConfiguration.getMongoSourceMongoDao().getRecordsFromList(failedRecordsUrls);
    } else {
      return basicConfiguration.getMongoSourceMongoDao()
          .getNextPageOfRecords(datasetId, nextPage);
    }
  }

  private String processAndIndex(FullBeanImpl fullBean) {
    try {
      final RDF rdf = processRecord(fullBean);
      indexRecord(rdf);
    } catch (ProcessingException e) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "{} - Could not process record: {}", prefixDatasetidLog,
          fullBean.getAbout(), e);
      return exceptionStacktraceToString(e);
    } catch (IndexingException e) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "{} - Could not index record: {}", prefixDatasetidLog,
          fullBean.getAbout(), e);
      return exceptionStacktraceToString(e);
    }
    return "";
  }

  private void updateProcessFailedOnlyCounts(String exceptionStackTrace, String resourceId) {
    updateProcessCounts(exceptionStackTrace, true, resourceId);
  }

  private void updateProcessCounts(String exceptionStackTrace, String resourceId) {
    updateProcessCounts(exceptionStackTrace, false, resourceId);
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
      String resourceId) {
    synchronized (this) {
      if (StringUtils.isNotBlank(resourceId)) {
        if (StringUtils.isBlank(exceptionStackTrace)) {
          if (processFailedOnly) {
            //Replace the already existent failed record with indication that it was successfully reprocessed
            final FailedRecord failedRecord = new FailedRecord(resourceId);
            failedRecord.setSuccessfullyReprocessed(true);
            basicConfiguration.getMongoDestinationMongoDao().storeFailedRecordToDb(failedRecord);
            datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() - 1);
            basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
          } else {
            datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
          }
        } else {
          basicConfiguration.getMongoDestinationMongoDao()
              .storeFailedRecordToDb(new FailedRecord(resourceId, exceptionStackTrace));
          if (!processFailedOnly) {
            datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
            datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
          }
        }
      }
    }
  }

  private RDF processRecord(FullBeanImpl fullBean)
      throws ProcessingException {
    final long startTimeProcess = System.nanoTime();
    try {
      return basicConfiguration.getExtraConfiguration().getFullBeanProcessor()
          .apply(fullBean, basicConfiguration);
    } finally {
      final long endTimeProcess = System.nanoTime();
      final long elapsedTime = endTimeProcess - startTimeProcess;
      synchronized (this) {
        datasetStatus.setTotalTimeProcessingInSecs(
            datasetStatus.getTotalTimeProcessingInSecs() + nanoTimeToSeconds(elapsedTime));
      }
    }
  }

  private void indexRecord(RDF rdf)
      throws IndexingException {
    final long startTimeIndex = System.nanoTime();
    try {
      basicConfiguration.getExtraConfiguration().getRdfIndexer()
          .accept(rdf, true, basicConfiguration);
    } finally {
      final long endTimeIndex = System.nanoTime();
      final long elapsedTime = endTimeIndex - startTimeIndex;
      synchronized (this) {
        datasetStatus.setTotalTimeIndexingInSecs(
            datasetStatus.getTotalTimeIndexingInSecs() + nanoTimeToSeconds(elapsedTime));
      }
    }
  }

  private void afterReProcess() {
    try {
      basicConfiguration.getExtraConfiguration().getAfterReprocessProcessor()
          .accept(datasetId, datasetStatus.getStartDate(), datasetStatus.getEndDate(),
              basicConfiguration);
    } catch (ProcessingException e) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "{} - After reprocessing operation failed!",
          prefixDatasetidLog, e);
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
