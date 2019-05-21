package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import eu.europeana.metis.reprocessing.model.Mode;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessForDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessForDataset.class);
  private final String prefixDatasetidLog;
  private final String datasetId;
  private final DatasetStatus datasetStatus;
  private final BasicConfiguration basicConfiguration;

  public ReprocessForDataset(String datasetId, int indexInOrderedList,
      BasicConfiguration basicConfiguration) {
    this.datasetId = datasetId;
    this.basicConfiguration = basicConfiguration;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
    this.datasetStatus = retrieveOrInitializeDatasetStatus(indexInOrderedList);
  }

  @Override
  public Void call() {
    return reprocessDataset();
  }

  private Void reprocessDataset() {
    LOGGER.info("{}, Processing started", prefixDatasetidLog);
    boolean processFailedOnly = datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()
        && basicConfiguration.getMode() == Mode.REPROCESS_ALL_FAILED;
    if (datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()
        && basicConfiguration.getMode() == Mode.DEFAULT) {
      LOGGER.info("{} - Processing not started because it was already completely processed",
          prefixDatasetidLog);
      return null;
    } else if (processFailedOnly) {
      LOGGER.info("{} - Processing will happen only on previously failed records",
          prefixDatasetidLog);
    }
    loopOverAllRecordsAndProcess(datasetStatus, processFailedOnly);
    LOGGER.info("{} - Processing end", prefixDatasetidLog);
    LOGGER.info("{} - DatasetStatus - {}", prefixDatasetidLog, datasetStatus);
    LOGGER.info(STATISTICS_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetidLog,
        datasetStatus);
    return null;
  }

  private DatasetStatus retrieveOrInitializeDatasetStatus(int indexInOrderedList) {
    DatasetStatus retrievedDatasetStatus = basicConfiguration.getMongoDestinationMongoDao()
        .getDatasetStatus(datasetId);
    if (retrievedDatasetStatus == null) {
      retrievedDatasetStatus = new DatasetStatus();
      final long totalRecordsForDataset = basicConfiguration.getMongoSourceMongoDao()
          .getTotalRecordsForDataset(datasetId);
      retrievedDatasetStatus.setDatasetId(datasetId);
      retrievedDatasetStatus.setIndexInOrderedList(indexInOrderedList);
      retrievedDatasetStatus.setTotalRecords(totalRecordsForDataset);
      basicConfiguration.getMongoDestinationMongoDao()
          .storeDatasetStatusToDb(retrievedDatasetStatus);
    }
    return retrievedDatasetStatus;
  }

  private int getStartingNextPage(DatasetStatus datasetStatus, boolean processFailedOnly) {
    if (processFailedOnly) {
      //Always start from the beginning
      return 0;
    } else {
      final long totalProcessed = datasetStatus.getTotalProcessed();
      return (int) (totalProcessed / MongoSourceMongoDao.PAGE_SIZE);
    }
  }

  private void loopOverAllRecordsAndProcess(final DatasetStatus datasetStatus,
      boolean processFailedOnly) {
    int nextPage = getStartingNextPage(datasetStatus, processFailedOnly);
    if (!processFailedOnly) {
      datasetStatus.setStartDate(new Date());
    }
    if (processFailedOnly) {
      failedRecordsOperation(datasetStatus, nextPage);
    } else {
      defaultOperation(datasetStatus, nextPage);
    }
  }

  private void failedRecordsOperation(DatasetStatus datasetStatus, int nextPage) {
    final long totalFailedRecords = datasetStatus.getTotalFailedRecords();
    List<FullBeanImpl> nextPageOfRecords = getFailedFullBeans(nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        final boolean successfulProcess = processAndIndex(datasetStatus, fullBean);
        updateProcessFailedOnlyCounts(successfulProcess, fullBean.getAbout());
      }
      LOGGER.info("{} - Processed number of records: {} out of total number of failed records: {}",
          prefixDatasetidLog, nextPageOfRecords.size(), totalFailedRecords);
      nextPage++;
      nextPageOfRecords = getFailedFullBeans(nextPage);
    }
  }

  private void defaultOperation(DatasetStatus datasetStatus, int nextPage) {
    List<FullBeanImpl> nextPageOfRecords = getFullBeans(nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      final long totalProcessedOld = datasetStatus.getTotalProcessed();
      final long totalTimeProcessingBefore = datasetStatus.getTotalTimeProcessing();
      final long totalTimeIndexingBefore = datasetStatus.getTotalTimeIndexing();
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        final boolean successfulProcess = processAndIndex(datasetStatus, fullBean);
        updateProcessCounts(successfulProcess, fullBean.getAbout());
      }
      LOGGER.info("{} - Processed number of records: {} out of total number of records: {}",
          prefixDatasetidLog, datasetStatus.getTotalProcessed(), datasetStatus.getTotalRecords());
      final long totalProcessedNew = datasetStatus.getTotalProcessed();
      final long totalTimeProcessingAfter = datasetStatus.getTotalTimeProcessing();
      final long totalTimeIndexingAfter = datasetStatus.getTotalTimeIndexing();
      final long newAverageProcessing = updateAverageWithNewValues(
          datasetStatus.getAverageTimeRecordProcessing(), totalProcessedOld,
          totalTimeProcessingAfter - totalTimeProcessingBefore, totalProcessedNew);
      final long newAverageIndexing = updateAverageWithNewValues(
          datasetStatus.getAverageTimeRecordIndexing(), totalProcessedOld,
          totalTimeIndexingAfter - totalTimeIndexingBefore, totalProcessedNew);
      datasetStatus.setAverageTimeRecordProcessing(newAverageProcessing);
      datasetStatus.setAverageTimeRecordIndexing(newAverageIndexing);
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      nextPage++;
      nextPageOfRecords = getFullBeans(nextPage);
    }
    //Set End Date
    datasetStatus.setEndDate(new Date());
    basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    try {
      basicConfiguration.getExtraConfiguration().getAfterReprocessProcessor()
          .accept(datasetId, datasetStatus.getStartDate(), datasetStatus.getEndDate(),
              basicConfiguration);
    } catch (ProcessingException e) {
      LOGGER.error("{} - After reprocessing operation failed!", prefixDatasetidLog, e);
    }
  }

  private List<FullBeanImpl> getFailedFullBeans(int nextPage) {
    return getFullBeans(nextPage, true);
  }

  private List<FullBeanImpl> getFullBeans(int nextPage) {
    return getFullBeans(nextPage, false);
  }

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

  private boolean processAndIndex(DatasetStatus datasetStatus, FullBeanImpl fullBean) {
    try {
      final RDF rdf = processRecord(fullBean, datasetStatus);
      indexRecord(rdf, datasetStatus);
    } catch (ProcessingException | IndexingException e) {
      String stepString;
      if (e instanceof ProcessingException) {
        stepString = "process";
      } else {
        stepString = "index";
      }
      LOGGER.error("{} - Could not {} record: {}", prefixDatasetidLog, stepString,
          fullBean.getAbout(), e);
      return false;
    }
    return true;
  }

  private void updateProcessFailedOnlyCounts(boolean successfulProcess, String resourceId) {
    updateProcessCounts(successfulProcess, true, resourceId);
  }

  private void updateProcessCounts(boolean successfulProcess, String resourceId) {
    updateProcessCounts(successfulProcess, false, resourceId);
  }

  private void updateProcessCounts(boolean successfulProcess, boolean processFailedOnly,
      String resourceId) {
    if (StringUtils.isNotBlank(resourceId)) {
      if (successfulProcess) {
        if (processFailedOnly) {
          basicConfiguration.getMongoDestinationMongoDao()
              .deleteFailedRecord(new FailedRecord(resourceId));
          datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() - 1);
          basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
        } else {
          datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
        }
      } else {
        basicConfiguration.getMongoDestinationMongoDao()
            .storeFailedRecordToDb(new FailedRecord(resourceId));
        if (!processFailedOnly) {
          datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
        }
      }
    }
  }

  private RDF processRecord(FullBeanImpl fullBean, DatasetStatus datasetStatus)
      throws ProcessingException {
    final long startTimeProcess = System.nanoTime();
    try {
      return basicConfiguration.getExtraConfiguration().getFullBeanProcessor()
          .apply(fullBean, basicConfiguration);
    } finally {
      final long endTimeProcess = System.nanoTime();
      final long elapsedTime = endTimeProcess - startTimeProcess;
      datasetStatus.setTotalTimeProcessing(datasetStatus.getTotalTimeProcessing() + elapsedTime);
    }
  }

  private void indexRecord(RDF rdf, DatasetStatus datasetStatus)
      throws IndexingException {
    final long startTimeIndex = System.nanoTime();
    try {
      basicConfiguration.getExtraConfiguration().getRdfIndexer()
          .accept(rdf, true, basicConfiguration);
    } finally {
      final long endTimeIndex = System.nanoTime();
      final long elapsedTime = endTimeIndex - startTimeIndex;
      datasetStatus.setTotalTimeIndexing(datasetStatus.getTotalTimeIndexing() + elapsedTime);
    }
  }

  private long updateAverageWithNewValues(long oldAverage, long oldTotalSamples,
      long sumOfNewValues,
      long newNumberOfSamples) {
    return (oldAverage * oldTotalSamples + sumOfNewValues) / (oldTotalSamples
        + newNumberOfSamples);
  }
}
