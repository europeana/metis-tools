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
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.remove.utils.Application;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepublishDatasetsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublishDatasetsMain.class);

  public static void main(String[] args)
      throws TrustStoreConfigurationException, IndexingException, URISyntaxException, IOException {

    final List<String> datasetIds = Arrays
        .asList("00501", "09330", "09411", "2020902", "2022069", "2022602", "2022607", "2026111",
            "2026121", "2032008", "2048216", "2051916", "2055701", "2055702", "2055703", "2055704",
            "2055705", "2055706", "2055707", "2055708", "2055709", "2055710", "2055711", "2055712",
            "2055713", "2055714", "2055715", "2055716", "2055717", "2055718", "2055719", "2055720",
            "2055721", "2055723", "2055724", "2055725", "2055726", "2055727", "2055728", "2055729",
            "2055730", "2055731", "2055732", "2055733", "2055738", "2055739", "9200109", "9200132",
            "9200175", "9200190", "9200203", "9200218", "9200226", "9200418", "9200419", "9200420");
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

    // Mark as deprecated
    final WorkflowExecutionDao workflowExecutionDao = new WorkflowExecutionDao(
        application.getDatastoreProvider());
    final AbstractExecutablePlugin targetPlugin = workflowExecutionDao
        .getLastFinishedWorkflowExecutionPluginByDatasetIdAndPluginType(datasetId,
            EnumSet.of(ExecutablePluginType.PUBLISH), true);
    if (targetPlugin != null) {
      final WorkflowExecution execution = workflowExecutionDao
          .getByExternalTaskId(Long.parseLong(targetPlugin.getExternalTaskId()));
      final Optional<AbstractMetisPlugin> metisPluginWithType = execution
          .getMetisPluginWithType(targetPlugin.getPluginType());
      metisPluginWithType.ifPresent(
          plugin -> ((AbstractExecutablePlugin) plugin).setDataStatus(DataStatus.DEPRECATED));
      workflowExecutionDao.update(execution);
    }

    // Remove from mongo and solr.
    final int removed = indexer.removeAll(datasetId);
    LOGGER.info("{} records in dataset {} are successfully removed.", removed, datasetId);
  }
}
