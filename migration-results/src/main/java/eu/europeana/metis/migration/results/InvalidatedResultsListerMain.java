package eu.europeana.metis.migration.results;

import com.opencsv.CSVWriter;
import eu.europeana.metis.migration.results.model.MigrationResult;
import eu.europeana.metis.migration.results.model.MigrationResult.ResultStatus;
import eu.europeana.metis.migration.results.model.MigrationResults;
import eu.europeana.metis.migration.results.model.PluginType;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

public class InvalidatedResultsListerMain {

  private static final String INVALIDATED_REPORT = "/home/jochen/migration/new_batch_report/invalidated_report.csv";

  private static final String SPLIT_RUN = MigrationResultParser.BATCH_5_FIRST_RUN_ID;

  public static void main(String[] args) throws IOException {

    // Obtain the model.
    final MigrationResults before = MigrationResultParser
        .parse(MigrationResultParser.PRE_HARVEST_RUN_ID, SPLIT_RUN);
    final MigrationResults after = MigrationResultParser.parseFrom(SPLIT_RUN);

    // Write the different plugins.
    try (
        final Writer writer = Files.newBufferedWriter(Paths.get(INVALIDATED_REPORT));
        final CSVWriter csvWriter = new CSVWriter(writer,
            CSVWriter.DEFAULT_SEPARATOR,
            CSVWriter.DEFAULT_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER,
            CSVWriter.DEFAULT_LINE_END)
    ) {
      csvWriter.writeNext(
          new String[]{"ID", "Name", "Plugin", "Run 1", "Plugin status 1", "Processed 1",
              "Errors 1", "Run 2", "Plugin status 2", "Processed 2", "Errors 2"});
      writeForPlugin(csvWriter, before.getMigrationResults(PluginType.OAIPMH_HARVEST),
          after.getMigrationResults(PluginType.OAIPMH_HARVEST));
      writeForPlugin(csvWriter, before.getMigrationResults(PluginType.PREVIEW),
          after.getMigrationResults(PluginType.PREVIEW));
      writeForPlugin(csvWriter, before.getMigrationResults(PluginType.PUBLISH),
          after.getMigrationResults(PluginType.PUBLISH));
    }
  }

  private static void writeForPlugin(CSVWriter csvWriter, Map<String, MigrationResult> before,
      Map<String, MigrationResult> after) {
    after.entrySet().stream().filter(entry -> before.containsKey(entry.getKey())).forEach(entry ->
        csvWriter.writeNext(
            createFullReportLine(entry.getKey(), before.get(entry.getKey()), entry.getValue()))
    );
  }

  private static String[] createFullReportLine(String datasetId, MigrationResult before,
      MigrationResult after) {

    // Create result and set id and name.
    final String[] result = new String[11];
    Arrays.fill(result, "");
    result[0] = datasetId;
    result[1] = after.getDatasetInfo().getNameInCsv();
    result[2] = after.getPluginType().toString();

    // Set before info
    result[3] = before.getRunId();
    if (!ResultStatus.DID_NOT_END_NORMALLY.test(before)) {
      // In case we have a valid result
      result[4] =
          ResultStatus.COMPLETED_WITH_ERRORS.test(before) ? "COMPLETED_WITH_ERRORS" : "SUCCEEDED";
      result[5] = before.getProcessedRecords().toString();
      result[6] = before.getErrorRecords().toString();
    } else {
      // In case something went wrong
      result[4] = "FAILED";
    }

    // Set after info
    result[7] = after.getRunId();
    if (!ResultStatus.DID_NOT_END_NORMALLY.test(after)) {
      // In case we have a valid result
      result[8] =
          ResultStatus.COMPLETED_WITH_ERRORS.test(after) ? "COMPLETED_WITH_ERRORS" : "SUCCEEDED";
      result[9] = after.getProcessedRecords().toString();
      result[10] = after.getErrorRecords().toString();
    } else {
      // In case something went wrong
      result[8] = "FAILED";
    }

    // Done
    return result;
  }
}
