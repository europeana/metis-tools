package eu.europeana.metis.migration.results;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.metis.migration.results.model.DatasetInfo;
import eu.europeana.metis.migration.results.model.MigrationResult;
import eu.europeana.metis.migration.results.model.MigrationResults;
import eu.europeana.metis.migration.results.model.PluginStatus;
import eu.europeana.metis.migration.results.model.PluginType;
import eu.europeana.metis.migration.results.model.TaskStatus;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class MigrationResultParser {

  private static final String DIRECTORY = "/home/jochen/migration/datasets-execution/logs";
  private static final String LOG_FILE_PREFIX = "final-dataset-status-";
  private static final String LOG_FILE_SUFFIX = ".log";

  private static final String DATASET_INFO_FILE = "/home/jochen/migration/dataset_info.json";

  static final String PRE_HARVEST_RUN_ID = "0000-00-00-000000";
  static final String BATCH_2_FIRST_RUN_ID = "2018-08-13-170336";
  static final String BATCH_3_FIRST_RUN_ID = "2018-08-23-091718";
  static final String BATCH_4_FIRST_RUN_ID = "2018-08-27-162346";
  static final String BATCH_5_FIRST_RUN_ID = "2018-08-31-124014";
  static final String BATCH_6_FIRST_RUN_ID = "2018-09-17-091345";
  static final String BATCH_7_FIRST_RUN_ID = "2018-09-27-093921";
  static final String FUTURE_RUN_ID = "9999-99-99-999999";

  private static Map<String, Integer> PRE_PROCESSED_DATASETS = new HashMap<>();

  static {
    PRE_PROCESSED_DATASETS.put("9200505", 785);
    PRE_PROCESSED_DATASETS.put("9200506", 1970);
    PRE_PROCESSED_DATASETS.put("9200574", 3);
    PRE_PROCESSED_DATASETS.put("2022111", 49174);
    PRE_PROCESSED_DATASETS.put("15416", 1568);
    PRE_PROCESSED_DATASETS.put("9200579", 92483);
    PRE_PROCESSED_DATASETS.put("08611", 39426);
  }

  private MigrationResultParser() {
  }

  static MigrationResults parseAll() throws IOException {
    return parse(PRE_HARVEST_RUN_ID, FUTURE_RUN_ID);
  }

  static MigrationResults parseFrom(String runId) throws IOException {
    return parse(runId, FUTURE_RUN_ID);
  }

  static MigrationResults parse(String fromRunId, String toRunId) throws IOException {
    final File directory = new File(DIRECTORY);
    final List<File> files = Stream.of(directory.listFiles())
        .filter(file -> file.getName().startsWith(LOG_FILE_PREFIX))
        .filter(file -> file.getName().endsWith(LOG_FILE_SUFFIX))
        .filter(file -> getRunId(file).compareTo(fromRunId) >= 0)
        .filter(file -> getRunId(file).compareTo(toRunId) < 0)
        .sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
    return parse(files, PRE_HARVEST_RUN_ID.compareTo(fromRunId) >= 0);
  }

  static Map<String, DatasetInfo> readDatasetInfo() throws IOException {
    System.out.println("Reading dataset info.");
    final ObjectMapper mapper = new ObjectMapper();
    final JavaType type = mapper.getTypeFactory()
        .constructParametricType(Map.class, String.class, DatasetInfo.class);
    return Collections.unmodifiableMap(mapper.readValue(new File(DATASET_INFO_FILE), type));
  }

  private static MigrationResults parse(List<File> files, boolean includePreProcessResults)
      throws IOException {

    // Read the dataset info.
    final Map<String, DatasetInfo> datasetInfo = readDatasetInfo();

    // Add pre precessed model
    final MigrationResults migrationResults = new MigrationResults();
    if (includePreProcessResults) {
      System.out.println("Parsing run: " + PRE_HARVEST_RUN_ID);
      for (Entry<String, Integer> preProcessedDataset : PRE_PROCESSED_DATASETS.entrySet()) {
        final String datasetId = preProcessedDataset.getKey();
        final int datasetSize = preProcessedDataset.getValue();
        migrationResults.add(
            new MigrationResult(PRE_HARVEST_RUN_ID, datasetInfo.get(datasetId), datasetId,
                "UNKNOWN",
                "UNKNOWN", PluginType.OAIPMH_HARVEST, "UNKNOWN", PluginStatus.FINISHED, datasetSize,
                datasetSize, 0, TaskStatus.PROCESSED));
        migrationResults.add(
            new MigrationResult(PRE_HARVEST_RUN_ID, datasetInfo.get(datasetId), datasetId,
                "UNKNOWN",
                "UNKNOWN", PluginType.PREVIEW, "UNKNOWN", PluginStatus.FINISHED, datasetSize,
                datasetSize, 0, TaskStatus.PROCESSED));
      }
    }

    // Process the files.
    files.forEach(file -> processFile(file, migrationResults, datasetInfo));

    // Done
    return migrationResults;
  }

  private static String getRunId(File file) {
    return file.getName()
        .substring(LOG_FILE_PREFIX.length(), file.getName().length() - LOG_FILE_SUFFIX.length());
  }

  private static void processFile(File file, MigrationResults migrationResults,
      Map<String, DatasetInfo> datasetInfo) {

    // Get the run id. This part of the file name should consists of a date followed by a time. E.g.
    // 2018-07-16-182455. So they are strictly increasing.
    final String runId = getRunId(file);
    System.out.println("Parsing run: " + runId);

    // Add each file to the migration model.
    try (InputStream is = Files.newInputStream(file.toPath(), StandardOpenOption.READ)) {
      final InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
      final BufferedReader lineReader = new BufferedReader(reader);
      String line;
      while ((line = lineReader.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          migrationResults.add(new MigrationResult(runId, datasetInfo::get, line));
        }
      }
    } catch (IOException e) {
      System.out.println("Could not read file " + file.getName());
      e.printStackTrace();
    }
  }
}
