package eu.europeana.metis.remove.depublish;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.MetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepublishDatasetsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublishDatasetsMain.class);

  public static void main(String[] args)
      throws IndexingException, URISyntaxException, IOException, TrustStoreConfigurationException {

    final List<String> datasetIds = Arrays.asList(
        // ADD DATASETS HERE.
    );
    //    FileUtils.readLines(new File(DATASET_IDS_FILE), StandardCharsets.UTF_8).stream()
    //        .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());
    try (final Application application = Application.initialize()) {
      final IndexingSettings settings = application.getProperties().getPublishIndexingSettings();
      try (final Indexer indexer = new IndexerFactory(settings).getIndexer()) {
        int count = 0;
        for (String datasetId : datasetIds) {
          count++;
          LOGGER
              .info("Depublishing dataset {} of {}. ID: {}.", count, datasetIds.size(), datasetId);
          depublish(datasetId, application, indexer);
        }
        LOGGER.info("Triggering flush of pending changes");
        indexer.triggerFlushOfPendingChanges(true);
      }
    }
  }

  private static void depublish(String datasetId, Application application, Indexer indexer)
      throws IndexingException {

    // Remove from mongo and solr.
    final int removed = indexer.removeAll(datasetId, null);
    LOGGER.info(" -> {} records in dataset {} are successfully removed.", removed, datasetId);

    // Mark as deprecated
    final WorkflowExecutionDao workflowExecutionDao = new WorkflowExecutionDao(
        application.getDatastoreProvider());
    final MetisPlugin targetPlugin = workflowExecutionDao.getLatestSuccessfulPlugin(datasetId,
        EnumSet.of(PluginType.PUBLISH, PluginType.REINDEX_TO_PUBLISH)).getPlugin();
    if (targetPlugin instanceof ExecutablePlugin) {
      final ExecutablePlugin castPlugin = (ExecutablePlugin) targetPlugin;
      final DataStatus dataStatus = castPlugin.getDataStatus();
      if (dataStatus == null || dataStatus == DataStatus.VALID) {
        final WorkflowExecution execution = workflowExecutionDao
            .getByExternalTaskId(Long.parseLong(castPlugin.getExternalTaskId()));
        final Optional<AbstractMetisPlugin> metisPluginWithType = execution
            .getMetisPluginWithType(castPlugin.getPluginType());
        metisPluginWithType.ifPresent(
            plugin -> ((AbstractExecutablePlugin) plugin).setDataStatus(DataStatus.DEPRECATED));
        workflowExecutionDao.update(execution);
      }
    }
  }
}
