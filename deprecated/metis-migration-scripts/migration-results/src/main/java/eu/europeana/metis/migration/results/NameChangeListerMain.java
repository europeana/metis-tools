package eu.europeana.metis.migration.results;

import com.opencsv.CSVWriter;
import eu.europeana.metis.migration.results.model.DatasetInfo;
import eu.europeana.metis.migration.results.model.MigrationResult;
import eu.europeana.metis.migration.results.model.MigrationResults;
import eu.europeana.metis.migration.results.model.PluginType;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * This class provides a report on all datasets that had their names changed, including those that
 * they conflicted with. And the model of the processing.
 */
public class NameChangeListerMain {

  private static final String DUPLICATE_NAME_REPORT = "/home/jochen/migration/new_batch_report/duplicate_name_report.csv";

  public static void main(String[] args) throws IOException {

    // Collect the harvesting model and dataset info
    final MigrationResults migrationResults = MigrationResultParser.parseAll();
    final Map<String, MigrationResult> harvestResults = migrationResults.getMigrationResults(
        PluginType.OAIPMH_HARVEST);
    final Map<String, DatasetInfo> datasetInfo = MigrationResultParser.readDatasetInfo();

    // Find those datasets that have a different name than in the source
    final List<Entry<String, DatasetInfo>> datasetsWithDifferentName = datasetInfo.entrySet()
        .stream()
        .filter(entry -> !entry.getValue().getNameInDb().equals(entry.getValue().getNameInCsv()))
        .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    System.out.println(String.format("Found %d datasets whose name has been changed.",
        datasetsWithDifferentName.size()));

    // Create map from old name to list of datasets and add the unchanged dataset with the same name.
    final Map<String, String> unchangedDatasetIdByName = datasetInfo.entrySet().stream()
        .filter(entry -> entry.getValue().getNameInDb().equals(entry.getValue().getNameInCsv()))
        .collect(Collectors.toMap(entry -> entry.getValue().getNameInDb(), Entry::getKey));
    final Map<String, List<Entry<String, DatasetInfo>>> datasetsByOriginalName = datasetsWithDifferentName
        .stream().collect(Collectors.groupingBy(entry -> entry.getValue().getNameInCsv()));
    for (Entry<String, List<Entry<String, DatasetInfo>>> entry : datasetsByOriginalName
        .entrySet()) {
      final String unchangedDatasetId = unchangedDatasetIdByName.get(entry.getKey());
      if (unchangedDatasetId != null) {
        entry.getValue().add(0,
            new SimpleImmutableEntry<>(unchangedDatasetId, datasetInfo.get(unchangedDatasetId)));
      }
    }

    // Write the model.
    {
      try (
          final Writer writer = Files.newBufferedWriter(Paths.get(DUPLICATE_NAME_REPORT));
          final CSVWriter csvWriter = new CSVWriter(writer,
              CSVWriter.DEFAULT_SEPARATOR,
              CSVWriter.DEFAULT_QUOTE_CHARACTER,
              CSVWriter.DEFAULT_ESCAPE_CHARACTER,
              CSVWriter.DEFAULT_LINE_END);
      ) {
        csvWriter.writeNext(
            new String[]{"Original name", "Unique name", "ID", "Harvested", "Processed records"});
        datasetsByOriginalName.entrySet().stream().sorted(Entry.comparingByKey())
            .forEach(entry -> createLines(csvWriter, entry, harvestResults));
      }
    }
  }

  private static void createLines(CSVWriter csvWriter,
      Entry<String, List<Entry<String, DatasetInfo>>> entries,
      Map<String, MigrationResult> harvestResults) {

    // Write empty line
    final String[] emptyLine = new String[5];
    Arrays.fill(emptyLine, "");
    csvWriter.writeNext(emptyLine);

    // Write the content
    for (Entry<String, DatasetInfo> entry : entries.getValue()) {
      csvWriter.writeNext(
          createLine(entry.getKey(), entry.getValue(), harvestResults.get(entry.getKey())));
    }
  }

  private static String[] createLine(String datasetId, DatasetInfo datasetInfo,
      MigrationResult harvest) {
    final String[] result = new String[5];
    Arrays.fill(result, "");
    result[0] = datasetInfo.getNameInCsv();
    result[1] = datasetInfo.getNameInDb();
    result[2] = datasetId;
    result[3] = harvest == null ? "No" : "Yes";
    result[4] = harvest == null ? "" : harvest.getProcessedRecords().toString();
    return result;
  }
}
