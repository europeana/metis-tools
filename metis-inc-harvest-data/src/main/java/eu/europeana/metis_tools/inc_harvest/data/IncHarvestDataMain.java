package eu.europeana.metis_tools.inc_harvest.data;

import eu.europeana.metis.core.dao.DataEvolutionUtils;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Looks at a OAI-PMH endpoint and harvest the one record that we need.
 */
public class IncHarvestDataMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IncHarvestDataMain.class);

  private static final Set<ExecutablePluginType> HARVEST_PLUGIN_TYPES = Collections.unmodifiableSet(
          EnumSet.of(ExecutablePluginType.OAIPMH_HARVEST, ExecutablePluginType.HTTP_HARVEST));

  private final PropertiesHolder propertiesHolder = new PropertiesHolder();

  private final Map<String, DatasetState> datasets = new HashMap<>();

  public static void main(String[] args) throws TrustStoreConfigurationException {
    final IncHarvestDataMain engine = new IncHarvestDataMain();
    engine.intialize();
    engine.readDatasets();
  }

  private void intialize() throws TrustStoreConfigurationException {

    // Append default truststore
    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.getTruststorePath()) && StringUtils
            .isNotEmpty(propertiesHolder.getTruststorePassword())) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.getTruststorePath(),
              propertiesHolder.getTruststorePassword());
    }
  }

  private void readDatasets() {

    // Get all dataset IDs
    LOGGER.info("Retrieving all dataset IDs.");
    final MongoClientProvider<IllegalArgumentException> mongoClientProvider = new MongoClientProvider<>(
            propertiesHolder.getMongoCoreProperties());
    new MongoCoreDao(mongoClientProvider, propertiesHolder.getMongoCoreDb()).getAllDatasetIds()
            .forEach(id -> datasets.put(id, new DatasetState()));

    // Get the various harvest and indexing operations
    LOGGER.info("Retrieving history for all datasets.");
    final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
            mongoClientProvider.createMongoClient(), propertiesHolder.getMongoCoreDb());
    final WorkflowExecutionDao workflowExecutionDao = new WorkflowExecutionDao(
            morphiaDatastoreProvider);
    final AtomicInteger counter = new AtomicInteger(0);
    datasets.forEach((id, state) -> {
      if (counter.incrementAndGet() % 50 == 0) {
        LOGGER.info("... {} of {} datasets processed.", counter, datasets.size());
      }
      readDataset(id, state, workflowExecutionDao);
    });



    LOGGER.info("Done.");
  }

  private void readDataset(String datasetId, DatasetState datasetState,
          WorkflowExecutionDao workflowExecutionDao) {

    // Get the latest harvest
    final PluginWithExecutionId<ExecutablePlugin> latestHarvest = workflowExecutionDao
            .getLatestSuccessfulExecutablePlugin(datasetId, HARVEST_PLUGIN_TYPES, false);
    if (latestHarvest!=null) {
      datasetState.setHarvestPlugin(latestHarvest.getPlugin());
    }

    // Get the latest preview and compute the root harvest for it.
    final PluginWithExecutionId<ExecutablePlugin> latestPreview = workflowExecutionDao
            .getLatestSuccessfulExecutablePlugin(datasetId,
                    EnumSet.of(ExecutablePluginType.PREVIEW), false);
    if (latestPreview != null) {
      datasetState.setPreviewPlugin(latestPreview.getPlugin());
      final ExecutablePlugin previewHarvest = new DataEvolutionUtils(workflowExecutionDao)
              .getRootAncestor(latestPreview).getPlugin();
      if (HARVEST_PLUGIN_TYPES
              .contains(previewHarvest.getPluginMetadata().getExecutablePluginType())) {
        datasetState.setPreviewHarvestPlugin(previewHarvest);
      }
    }

    // Get the latest publish and compute the root harvest for it.
    final PluginWithExecutionId<ExecutablePlugin> latestPublish = workflowExecutionDao
            .getLatestSuccessfulExecutablePlugin(datasetId,
                    EnumSet.of(ExecutablePluginType.PUBLISH), false);
    if (latestPublish != null) {
      datasetState.setPublishPlugin(latestPublish.getPlugin());
      final ExecutablePlugin publishHarvest = new DataEvolutionUtils(workflowExecutionDao)
              .getRootAncestor(latestPublish).getPlugin();
      if (HARVEST_PLUGIN_TYPES
              .contains(publishHarvest.getPluginMetadata().getExecutablePluginType())) {
        datasetState.setPublishedHarvestPlugin(publishHarvest);
      }
    }
  }
}
