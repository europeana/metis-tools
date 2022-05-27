package eu.europeana.metis.remove.datasetanalysis;

import com.mongodb.client.MongoCollection;
import com.opencsv.CSVWriter;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class analyzes the data and lists all datasets of which the latest executed plugin
 * is in one of the listed types.
 */
public class GetDatasetsForLatestPluginTypeMain {

  // Where I want the output file to be located.
  private static final String OUTPUT_FILE = "/home/jochen/Desktop/dataset_analysis.csv";

  // I want to disregard link checking, reindexing and depublication and such.
  private static final Set<ExecutablePluginType> meaningfulTypes = Set.of(
      ExecutablePluginType.OAIPMH_HARVEST, ExecutablePluginType.HTTP_HARVEST,
      ExecutablePluginType.VALIDATION_EXTERNAL, ExecutablePluginType.TRANSFORMATION,
      ExecutablePluginType.VALIDATION_INTERNAL, ExecutablePluginType.NORMALIZATION,
      ExecutablePluginType.ENRICHMENT, ExecutablePluginType.MEDIA_PROCESS,
      ExecutablePluginType.PREVIEW, ExecutablePluginType.PUBLISH);

  // I want only the datasets who are currently in one of these types.
  private final static Set<PluginType> permissibleTypes = Set.of(PluginType.ENRICHMENT,
      PluginType.MEDIA_PROCESS, PluginType.PREVIEW);

  private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
      .withZone(ZoneId.systemDefault());

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    try (final Application application = Application.initialize()) {
      final Set<String> datasetIds = getDatasetIds(application.getDatastoreProvider());
      final WorkflowExecutionDao dao = new WorkflowExecutionDao(application.getDatastoreProvider());
      final AtomicInteger counter = new AtomicInteger();
      final List<DatasetResult> results = new ArrayList<>();
      datasetIds.forEach(datasetId -> {
        final PluginWithExecutionId<ExecutablePlugin> latestPlugin = dao.getLatestSuccessfulExecutablePlugin(
            datasetId, meaningfulTypes, false);
        if (latestPlugin != null &&
            permissibleTypes.contains(latestPlugin.getPlugin().getPluginType())) {
          results.add(new DatasetResult(datasetId, latestPlugin.getPlugin()));
        }
        final int currentCounter = counter.incrementAndGet();
        if (currentCounter % 100 == 0) {
          System.out.println(
              "Analyzed " + currentCounter + " of " + datasetIds.size() + " datasets.");
        }
      });
      outputResults(results);
    }
  }

  private static Set<String> getDatasetIds(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    final MongoCollection<WorkflowExecution> collection = morphiaDatastoreProvider.getDatastore()
        .getMapper().getCollection(WorkflowExecution.class);
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
      final Set<String> datasetIds = new HashSet<>();
      collection.distinct("datasetId", String.class).cursor().forEachRemaining(datasetIds::add);
      return datasetIds;
    });
  }

  private static void outputResults(List<DatasetResult> results) throws IOException {
    final Path path = Paths.get(OUTPUT_FILE);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "datasetId",
          "last executed plugin",
          "time of plugin execution",
          "number of records processed"
      });

      // Write records
      results.forEach(result ->
          writer.writeNext(new String[]{
              result.getDatasetId(),
              result.getLatestPlugin().getPluginType().name(),
              dateFormatter.format(result.getLatestPlugin().getFinishedDate().toInstant()),
              "" + result.getLatestPlugin().getExecutionProgress().getProcessedRecords()
          })
      );
    }
  }

  private static class DatasetResult {

    private final String datasetId;
    private final ExecutablePlugin latestPlugin;

    public DatasetResult(String datasetId, ExecutablePlugin latestPlugin) {
      this.datasetId = datasetId;
      this.latestPlugin = latestPlugin;
    }

    public String getDatasetId() {
      return datasetId;
    }

    public ExecutablePlugin getLatestPlugin() {
      return latestPlugin;
    }
  }
}
