package eu.europeana.metis.reprocessing.utilities;

import com.apicatalog.jsonld.StringUtils;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPluginMetadata;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.config.DefaultConfiguration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.bson.types.ObjectId;

/**
 * Contains functionality for the after a reprocessing operation of a dataset.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the after reprocessing operation.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-22
 */
public class PostProcessUtilities {

  private static final String WHITE_LIST_REPROCESS_PLUGIN ="VALIDATION_INTERNAL";
  private static final String WHITE_LIST_INVALIDATION = "NORMALIZATION,ENRICHMENT,MEDIA_PROCESS,PREVIEW,PUBLISH";
  private static final String NON_WHITE_LIST_REPROCESS_PLUGIN ="MEDIA_PROCESS";
  private static final String NON_WHITE_LIST_INVALIDATION = "PREVIEW,PUBLISH";

  private PostProcessUtilities() {
  }

  /**
   * It writes all the required information to metis core of a dataset that has just been
   * re-processed. It is meant to be run once after the process and index operations of all records
   * in that dataset.
   *
   * @param datasetId the dataset id of the finished dataset re-processing
   * @param startDate the start date of the re-processing
   * @param endDate the end date of the re-processing
   * @param configuration the configuration class that contains required properties
   */
  public static void postProcess(String datasetId, Instant startDate, Instant endDate, Configuration configuration) {
    
    if (DefaultConfiguration.isDatasetOnWhitelist(datasetId)) {
      configuration.setInvalidatePluginTypes(Arrays
          .stream(WHITE_LIST_INVALIDATION.split(","))
          .filter(StringUtils::isNotBlank).map(String::trim)
          .map(ExecutablePluginType::getPluginTypeFromEnumName).toList());
      configuration.setReprocessBasedOnPluginType(ExecutablePluginType
          .getPluginTypeFromEnumName(WHITE_LIST_REPROCESS_PLUGIN));
    } else {
      configuration.setInvalidatePluginTypes(Arrays
          .stream(NON_WHITE_LIST_INVALIDATION.split(","))
          .filter(StringUtils::isNotBlank).map(String::trim)
          .map(ExecutablePluginType::getPluginTypeFromEnumName).toList());
      configuration.setReprocessBasedOnPluginType(ExecutablePluginType
          .getPluginTypeFromEnumName(NON_WHITE_LIST_REPROCESS_PLUGIN));
    }
    updateMetisCoreWorkflowExecutions(datasetId, startDate, endDate, configuration);
  }

  public static void updateMetisCoreWorkflowExecutions(String datasetId, Instant startDate, Instant endDate, Configuration configuration) {
    createReindexWorkflowExecutions(datasetId, startDate, endDate, configuration);
    setInvalidFlagToPlugins(datasetId, configuration);
  }

  private static void createReindexWorkflowExecutions(String datasetId, Instant startDate,
      Instant endDate, Configuration configuration) {
    final PluginWithExecutionId<ExecutablePlugin> lastExecutionToBeBasedOn = configuration
        .getMetisCoreMongoDao().getWorkflowExecutionDao()
        .getLatestSuccessfulExecutablePlugin(datasetId,
            Collections.singleton(configuration.getReprocessBasedOnPluginType()), false);

    //Preview Plugin
    final ReindexToPreviewPluginMetadata reindexToPreviewPluginMetadata = new ReindexToPreviewPluginMetadata();
    reindexToPreviewPluginMetadata.setRevisionNamePreviousPlugin(
        lastExecutionToBeBasedOn == null ? null
            : lastExecutionToBeBasedOn.getPlugin().getPluginType().name());
    reindexToPreviewPluginMetadata.setRevisionTimestampPreviousPlugin(
        lastExecutionToBeBasedOn == null ? null
            : lastExecutionToBeBasedOn.getPlugin().getStartedDate());
    final ReindexToPreviewPlugin reindexToPreviewPlugin = new ReindexToPreviewPlugin(
        reindexToPreviewPluginMetadata);
    reindexToPreviewPlugin.setId(new ObjectId().toString() + "-" + reindexToPreviewPlugin.getPluginType().name());
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
    reindexToPublishPlugin.setId(new ObjectId().toString() + "-" + reindexToPublishPlugin.getPluginType().name());
    reindexToPublishPlugin.setStartedDate(startDate);
    reindexToPublishPlugin.setFinishedDate(endDate);
    reindexToPublishPlugin.setPluginStatus(PluginStatus.FINISHED);

    final Dataset dataset = configuration.getMetisCoreMongoDao().getDataset(datasetId);
    final List<AbstractMetisPlugin<?>> abstractMetisPlugins = new ArrayList<>();
    abstractMetisPlugins.add(reindexToPreviewPlugin);
    abstractMetisPlugins.add(reindexToPublishPlugin);
    final WorkflowExecution workflowExecution = new WorkflowExecution();
    workflowExecution.setDatasetId(dataset.getDatasetId());
    workflowExecution.setEcloudDatasetId(dataset.getEcloudDatasetId());
    workflowExecution.setMetisPlugins(abstractMetisPlugins);
    workflowExecution.setWorkflowStatus(WorkflowStatus.FINISHED);
    workflowExecution.setCreatedDate(startDate);
    workflowExecution.setStartedDate(startDate);
    workflowExecution.setUpdatedDate(endDate);
    workflowExecution.setFinishedDate(endDate);
    configuration.getMetisCoreMongoDao().getWorkflowExecutionDao().create(workflowExecution);
  }

  private static void setInvalidFlagToPlugins(String datasetId,
      Configuration configuration) {
    final List<ExecutablePluginType> invalidatePluginTypes = configuration.getInvalidatePluginTypes();
    final WorkflowExecutionDao workflowExecutionDao = configuration.getMetisCoreMongoDao()
                                                                   .getWorkflowExecutionDao();
    final List<PluginWithExecutionId<ExecutablePlugin>> deprecatedPlugins = invalidatePluginTypes
        .stream().map(executablePluginType -> workflowExecutionDao
            .getLatestSuccessfulExecutablePlugin(datasetId,
                Collections.singleton(executablePluginType), false)).filter(Objects::nonNull)
        .toList();

    deprecatedPlugins.stream().map(abstractExecutablePlugin -> {
      final WorkflowExecution workflowExecution = workflowExecutionDao.getByExternalTaskId(abstractExecutablePlugin.getPlugin().getExternalTaskId());
      final Optional<AbstractMetisPlugin<?>> metisPluginWithType = workflowExecution
          .getMetisPlugins()
          .stream()
          .filter(plugin ->
                  plugin.getPluginType() == abstractExecutablePlugin.getPlugin().getPluginType())
          .findFirst();

      metisPluginWithType.ifPresent(
          abstractMetisPlugin -> (abstractMetisPlugin).setDataStatus(DataStatus.DEPRECATED));
      return workflowExecution;
    }).forEach(workflowExecutionDao::update);
  }
}
