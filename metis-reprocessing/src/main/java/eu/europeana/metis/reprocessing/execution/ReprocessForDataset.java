package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPluginMetadata;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import eu.europeana.metis.reprocessing.model.Mode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
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
    List<FullBeanImpl> nextPageOfRecords = getFullBeans(nextPage, true);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        processAndIndex(datasetStatus, fullBean, true);
      }
      LOGGER.info("{} - Processed number of records: {} out of total number of failed records: {}",
          prefixDatasetidLog, nextPageOfRecords.size(), totalFailedRecords);
      nextPage++;
      nextPageOfRecords = getFullBeans(nextPage, true);
    }
  }

  private void defaultOperation(DatasetStatus datasetStatus, int nextPage) {
    List<FullBeanImpl> nextPageOfRecords = getFullBeans(nextPage, false);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      final long totalProcessedOld = datasetStatus.getTotalProcessed();
      final long totalTimeProcessingBefore = datasetStatus.getTotalTimeProcessing();
      final long totalTimeIndexingBefore = datasetStatus.getTotalTimeIndexing();
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        processAndIndex(datasetStatus, fullBean, false);
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
      nextPageOfRecords = getFullBeans(nextPage, false);
    }
    //Set End Date
    datasetStatus.setEndDate(new Date());
    basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    updateMetisCoreWorkflowExecutions(datasetStatus.getStartDate(), datasetStatus.getEndDate());
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

  private void processAndIndex(DatasetStatus datasetStatus, FullBeanImpl fullBean,
      boolean processFailedOnly) {
    try {
      final RDF rdf = processRecord(fullBean, datasetStatus);
      indexRecord(rdf, datasetStatus);
      if (processFailedOnly && fullBean.getAbout() != null) {
        basicConfiguration.getMongoDestinationMongoDao()
            .deleteFailedRecord(new FailedRecord(fullBean.getAbout()));
        datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() - 1);
        basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      }
    } catch (ProcessingException | IndexingException e) {
      String stepString;
      if (e instanceof ProcessingException) {
        stepString = "process";
      } else {
        stepString = "index";
      }
      LOGGER.error("{} - Could not {} record: {}", prefixDatasetidLog, stepString,
          fullBean.getAbout(), e);
      if (fullBean.getAbout() != null) {
        basicConfiguration.getMongoDestinationMongoDao()
            .storeFailedRecordToDb(new FailedRecord(fullBean.getAbout()));
        if (!processFailedOnly) {
          datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
        }
      }
    } finally {
      if (!processFailedOnly) {
        datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
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
    return (oldAverage * oldTotalSamples + sumOfNewValues) / (oldTotalSamples + newNumberOfSamples);
  }

  private void updateMetisCoreWorkflowExecutions(Date startDate, Date endDate) {
//    createReindexWorkflowExecutions(startDate, endDate);
//    setInvalidFlagToPlugins();
  }

  private void setInvalidFlagToPlugins() {
    final List<ExecutablePluginType> invalidatePluginTypes = basicConfiguration
        .getInvalidatePluginTypes();
    final WorkflowExecutionDao workflowExecutionDao = basicConfiguration.getMetisCoreMongoDao()
        .getWorkflowExecutionDao();
    final List<AbstractExecutablePlugin> deprecatedPlugins = invalidatePluginTypes.stream()
        .map(executablePluginType -> workflowExecutionDao
            .getLastFinishedWorkflowExecutionPluginByDatasetIdAndPluginType(datasetId,
                Collections.singleton(executablePluginType), false)).collect(Collectors.toList());

    deprecatedPlugins.stream().map(abstractExecutablePlugin -> {
      final WorkflowExecution workflowExecution = workflowExecutionDao
          .getByExternalTaskId(Long.parseLong(abstractExecutablePlugin.getExternalTaskId()));
      final Optional<AbstractMetisPlugin> metisPluginWithType = workflowExecution
          .getMetisPluginWithType(abstractExecutablePlugin.getPluginType());
      metisPluginWithType.ifPresent(
          abstractMetisPlugin -> ((AbstractExecutablePlugin) abstractMetisPlugin)
              .setDataStatus(DataStatus.DEPRECATED));
      return workflowExecution;
    }).forEach(workflowExecutionDao::update);
  }

  private void createReindexWorkflowExecutions(Date startDate, Date endDate) {
    final AbstractExecutablePlugin lastExecutionToBeBasedOn = basicConfiguration
        .getMetisCoreMongoDao().getWorkflowExecutionDao()
        .getLastFinishedWorkflowExecutionPluginByDatasetIdAndPluginType(datasetId,
            Collections.singleton(basicConfiguration.getReprocessBasedOnPluginType()), false);

    //Preview Plugin
    final ReindexToPreviewPluginMetadata reindexToPreviewPluginMetadata = new ReindexToPreviewPluginMetadata();
    reindexToPreviewPluginMetadata
        .setRevisionNamePreviousPlugin(lastExecutionToBeBasedOn.getPluginType().name());
    reindexToPreviewPluginMetadata
        .setRevisionTimestampPreviousPlugin(lastExecutionToBeBasedOn.getStartedDate());
    final ReindexToPreviewPlugin reindexToPreviewPlugin = new ReindexToPreviewPlugin(
        reindexToPreviewPluginMetadata);
    reindexToPreviewPlugin
        .setId(new ObjectId().toString() + "-" + reindexToPreviewPlugin.getPluginType().name());
    reindexToPreviewPlugin.setStartedDate(startDate);
    reindexToPreviewPlugin.setFinishedDate(endDate);
    reindexToPreviewPlugin.setPluginStatus(PluginStatus.FINISHED);

    //Publish Plugin
    final ReindexToPublishPluginMetadata reindexToPublishPluginMetadata = new ReindexToPublishPluginMetadata();
    reindexToPublishPluginMetadata
        .setRevisionNamePreviousPlugin(reindexToPreviewPlugin.getPluginType().name());
    reindexToPublishPluginMetadata
        .setRevisionTimestampPreviousPlugin(reindexToPreviewPlugin.getStartedDate());
    final ReindexToPublishPlugin reindexToPublishPlugin = new ReindexToPublishPlugin(
        reindexToPublishPluginMetadata);
    reindexToPublishPlugin
        .setId(new ObjectId().toString() + "-" + reindexToPublishPlugin.getPluginType().name());
    reindexToPublishPlugin.setStartedDate(startDate);
    reindexToPublishPlugin.setFinishedDate(endDate);
    reindexToPublishPlugin.setPluginStatus(PluginStatus.FINISHED);

    final Dataset dataset = basicConfiguration.getMetisCoreMongoDao().getDataset(datasetId);
    final ArrayList<AbstractMetisPlugin> abstractMetisPlugins = new ArrayList<>();
    abstractMetisPlugins.add(reindexToPreviewPlugin);
    abstractMetisPlugins.add(reindexToPublishPlugin);
    final WorkflowExecution workflowExecution = new WorkflowExecution(dataset, abstractMetisPlugins,
        0);
    workflowExecution.setWorkflowStatus(WorkflowStatus.FINISHED);
    workflowExecution.setCreatedDate(startDate);
    workflowExecution.setStartedDate(startDate);
    workflowExecution.setUpdatedDate(endDate);
    workflowExecution.setFinishedDate(endDate);
    basicConfiguration.getMetisCoreMongoDao().getWorkflowExecutionDao().create(workflowExecution);
  }
}
