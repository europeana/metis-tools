package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
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
  private final BasicConfiguration basicConfiguration;

  public ReprocessForDataset(String datasetId, BasicConfiguration basicConfiguration) {
    this.datasetId = datasetId;
    this.basicConfiguration = basicConfiguration;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
  }

  @Override
  public Void call() {
    return reprocessDataset();
  }

  private Void reprocessDataset() {
    LOGGER.info("Processing DatasetId: {} started", datasetId);
    final DatasetStatus datasetStatus = retrieveOrInitializeDatasetStatus();
    if (datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()) {
      LOGGER
          .info("{} - Processing not started because it was already completely processed",
              prefixDatasetidLog);
      return null;
    }
    loopOverAllRecordsAndProcess(datasetStatus);
    LOGGER.info("{} - Processing end", prefixDatasetidLog);
    LOGGER.info("{} - DatasetStatus - {}", prefixDatasetidLog, datasetStatus);
    LOGGER.info(STATISTICS_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetidLog,
        datasetStatus);
    return null;
  }

  private DatasetStatus retrieveOrInitializeDatasetStatus() {
    DatasetStatus datasetStatus = basicConfiguration.getMongoDestinationMongoDao()
        .getDatasetStatus(datasetId);
    if (datasetStatus == null) {
      datasetStatus = new DatasetStatus();
      final long totalRecordsForDataset = basicConfiguration.getMongoSourceMongoDao()
          .getTotalRecordsForDataset(datasetId);
      datasetStatus.setDatasetId(datasetId);
      datasetStatus.setTotalRecords(totalRecordsForDataset);
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    }
    return datasetStatus;
  }

  private int getNextPage(DatasetStatus datasetStatus) {
    final long totalProcessed = datasetStatus.getTotalProcessed();
    return (int) (totalProcessed / MongoSourceMongoDao.PAGE_SIZE);
  }

  private void loopOverAllRecordsAndProcess(final DatasetStatus datasetStatus) {
    Date startedDate = new Date();
    int nextPage = getNextPage(datasetStatus);
    List<FullBeanImpl> nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
        .getNextPageOfRecords(datasetId, nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        processAndIndex(datasetStatus, fullBean);
      }
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      nextPage++;
      nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
          .getNextPageOfRecords(datasetId, nextPage);
    }
    updateMetisCoreWorkflowExecutions(startedDate);
  }

  private void processAndIndex(DatasetStatus datasetStatus, FullBeanImpl fullBean) {
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
      if (fullBean.getAbout() != null) {
        datasetStatus.getFailedRecordsSet().add(fullBean.getAbout());
        datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
      }
    } finally {
      datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
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
      final long newAverage = addValueToAverage(datasetStatus.getTotalProcessed() + 1,
          datasetStatus.getAverageTimeRecordProcessing(), elapsedTime);
      datasetStatus.setAverageTimeRecordProcessing(newAverage);
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
      final long newAverage = addValueToAverage(datasetStatus.getTotalProcessed() + 1,
          datasetStatus.getAverageTimeRecordIndexing(), elapsedTime);
      datasetStatus.setAverageTimeRecordIndexing(newAverage);
      datasetStatus.setTotalTimeIndexing(datasetStatus.getTotalTimeIndexing() + elapsedTime);
    }
  }

  private long addValueToAverage(long totalSamples, long oldAverage, long newValue) {
    return oldAverage + ((newValue - oldAverage) / totalSamples);
  }

  private void updateMetisCoreWorkflowExecutions(Date startedDate) {
    createReindexWorkflowExecutions(startedDate);
    setInvalidFlagToPlugins();
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

  private void createReindexWorkflowExecutions(Date startedDate) {
    final Date finishedDate = new Date();
    final ReindexToPreviewPlugin reindexToPreviewPlugin = new ReindexToPreviewPlugin(
        new ReindexToPreviewPluginMetadata());
    reindexToPreviewPlugin.setStartedDate(startedDate);
    reindexToPreviewPlugin.setFinishedDate(finishedDate);
    reindexToPreviewPlugin.setPluginStatus(PluginStatus.FINISHED);
    final ReindexToPublishPlugin reindexToPublishPlugin = new ReindexToPublishPlugin(
        new ReindexToPublishPluginMetadata());
    reindexToPublishPlugin.setStartedDate(startedDate);
    reindexToPublishPlugin.setFinishedDate(finishedDate);
    reindexToPublishPlugin.setPluginStatus(PluginStatus.FINISHED);
    final Dataset dataset = basicConfiguration.getMetisCoreMongoDao().getDataset(datasetId);
    final ArrayList<AbstractMetisPlugin> abstractMetisPlugins = new ArrayList<>();
    abstractMetisPlugins.add(reindexToPreviewPlugin);
    abstractMetisPlugins.add(reindexToPublishPlugin);
    final WorkflowExecution workflowExecution = new WorkflowExecution(dataset, abstractMetisPlugins,
        0);
    basicConfiguration.getMetisCoreMongoDao().getWorkflowExecutionDao().create(workflowExecution);
  }
}
