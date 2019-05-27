package eu.europeana.metis.reprocessing.execution;

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
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;

/**
 * Contains functionality for the after a reprocessing operation of a dataset.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the after reprocessing operation.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-22
 */
public class AfterReProcessingUtilities {

  private AfterReProcessingUtilities() {
  }

  /**
   * It writes all the required information to metis core of a dataset that has just been
   * re-processed. It is meant to be run once after the process and index operations of all records
   * in that dataset.
   *
   * @param datasetId the dataset id of the finished dataset re-processing
   * @param startDate the start date of the re-processing
   * @param endDate the end date of the re-processing
   * @param basicConfiguration the configuration class that contains required properties
   */
  public static void updateMetisCoreWorkflowExecutions(String datasetId, Date startDate,
      Date endDate, BasicConfiguration basicConfiguration) {
    // TODO: 21-5-19 Enable methods when ready
//    createReindexWorkflowExecutions(datasetId, startDate, endDate, basicConfiguration);
//    setInvalidFlagToPlugins(datasetId, basicConfiguration);
  }

  private static void createReindexWorkflowExecutions(String datasetId, Date startDate,
      Date endDate, BasicConfiguration basicConfiguration) {
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

  private static void setInvalidFlagToPlugins(String datasetId,
      BasicConfiguration basicConfiguration) {
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
}
