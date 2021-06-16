package eu.europeana.metis_tools.inc_harvest.data;

import com.opencsv.CSVWriter;
import eu.europeana.metis.core.dao.DataEvolutionUtils;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.core.workflow.plugins.MetisPlugin;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncHarvestDataMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IncHarvestDataMain.class);

  private static final Set<ExecutablePluginType> HARVEST_PLUGIN_TYPES = Collections.unmodifiableSet(
          EnumSet.of(ExecutablePluginType.OAIPMH_HARVEST, ExecutablePluginType.HTTP_HARVEST));

  private static final String OUTPUT_FILE = "/home/jochen/Desktop/test_migration/output.csv";

  private enum RecordPresence{PREVIEW, PUBLISH, BOTH}

  private final PropertiesHolder propertiesHolder = new PropertiesHolder();

  private final Map<String, DatasetState> datasets = new HashMap<>();

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    final Path outputFile = Path.of(OUTPUT_FILE);
    final IncHarvestDataMain engine = new IncHarvestDataMain();
    engine.intialize();
    engine.readDatasets();
    engine.readRecords(outputFile);
    LOGGER.info("Done.");
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

    // Get the various harvest and indexing operations from metis core.
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
  }

  private void readDataset(String datasetId, DatasetState datasetState,
          WorkflowExecutionDao workflowExecutionDao) {

    // Get the latest harvest
    final PluginWithExecutionId<ExecutablePlugin> latestHarvest = workflowExecutionDao
            .getLatestSuccessfulExecutablePlugin(datasetId, HARVEST_PLUGIN_TYPES, false);
    if (latestHarvest != null) {
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

  private void readRecords(Path outputFile) throws IOException {

    // Create database access
    final MongoRecordDao mongoPreviewRecordDao = new MongoRecordDao(
            new MongoClientProvider<>(propertiesHolder.getMongoPreviewProperties()),
            propertiesHolder.getPreviewMongoDb());
    final MongoRecordDao mongoPublishRecordDao = new MongoRecordDao(
            new MongoClientProvider<>(propertiesHolder.getMongoPublishProperties()),
            propertiesHolder.getPublishMongoDb());

    // Create output stream for destination file.
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
            final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
              "metis_dataset_id",
              "record_id",
              "latest_harvest_revision_timestamp",
              "preview_harvest_revision_timestamp",
              "published_harvest_revision_timestamp"
      });

      // Write the records for each dataset
      final AtomicInteger counter = new AtomicInteger(0);
      datasets.forEach((datasetId, datasetState) -> {
        LOGGER.info("Processing dataset {}: ({} of {}).", datasetId, counter.incrementAndGet(),
                datasets.size());
        final Map<String, RecordPresence> records = getRecordIds(datasetId, mongoPreviewRecordDao,
                mongoPublishRecordDao);
        writeRecords(datasetId, datasetState, records, writer);
      });
    }
  }

  private Map<String, RecordPresence> getRecordIds(String datasetId,
          MongoRecordDao mongoPreviewRecordDao, MongoRecordDao mongoPublishRecordDao) {

    // Find the records from preview
    final Map<String, RecordPresence> result = new HashMap<>();
    LOGGER.info("... Reading records from preview.");
    final AtomicInteger previewCounter = new AtomicInteger(0);
    mongoPreviewRecordDao.getAllRecordIds(datasetId).forEach(recordId -> {
      if (previewCounter.incrementAndGet() % 10000 == 0) {
        LOGGER.info("... ... {} records found.", previewCounter.get());
      }
      result.put(recordId, RecordPresence.PREVIEW);
    });

    // Find the reocrds from publish
    LOGGER.info("... Reading records from publish.");
    final AtomicInteger publishCounter = new AtomicInteger(0);
    mongoPublishRecordDao.getAllRecordIds(datasetId).forEach(recordId -> {
      if (publishCounter.incrementAndGet() % 10000 == 0) {
        LOGGER.info("... ... {} records found.", publishCounter.get());
      }
      result.merge(recordId, RecordPresence.PUBLISH, (v1, v2) -> RecordPresence.BOTH);
    });

    // Return the merged result.
    return result;
  }

  private void writeRecords(String datasetId, DatasetState datasetState,
          Map<String, RecordPresence> records, CSVWriter writer) {

    // Get the required information. If the dataset is not even harvested, we are done.
    final String latestHarvestTime = getStartedDate(datasetState.getHarvestPlugin());
    if (latestHarvestTime == null) {
      return;
    }
    final String previewHarvestTime = getStartedDate(datasetState.getPreviewHarvestPlugin());
    final String publishHarvestTime = getStartedDate(datasetState.getPublishedHarvestPlugin());

    // Go by all the records.
    LOGGER.info("... Writing records to the output.");
    final AtomicInteger counter = new AtomicInteger(0);
    records.forEach((recordId, presence) -> {

      // Counter
      if (counter.incrementAndGet() % 100000 == 0) {
        LOGGER.info("... ... {} records written.", counter.get());
      }

      // Do some checks first
      final boolean preview = presence == RecordPresence.PREVIEW || presence == RecordPresence.BOTH;
      final boolean publish = presence == RecordPresence.PUBLISH || presence == RecordPresence.BOTH;
      if (preview && previewHarvestTime == null) {
        LOGGER.warn("Record {} is in preview but no preview harvest is known.", recordId);
        return;
      }
      if (publish && publishHarvestTime == null) {
        LOGGER.warn("Record {} is in publish but no published harvest is known.", recordId);
        return;
      }

      // Add the record.
      writer.writeNext(new String[]{
              datasetId,
              recordId,
              latestHarvestTime,
              preview ? previewHarvestTime : null,
              publish ? publishHarvestTime : null
      });
    });
  }

  private static String getStartedDate(ExecutablePlugin plugin) {
    return Optional.ofNullable(plugin).map(MetisPlugin::getStartedDate).map(Date::getTime)
            .map(time -> Long.toString(time)).orElse(null);
  }
}
