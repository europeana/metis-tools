package eu.europeana.metis.migration.results;

import com.opencsv.CSVWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.solr.client.solrj.util.ClientUtils;
import eu.europeana.metis.migration.results.model.MigrationResult;
import eu.europeana.metis.migration.results.model.MigrationResult.ResultStatus;
import eu.europeana.metis.migration.results.model.MigrationResults;
import eu.europeana.metis.migration.results.model.PluginType;

/**
 * This class provides a report on a given batch. A full report and an error report.
 */
public class BatchReportCreatorMain {

  private static final String FULL_REPORT = "/home/jochen/migration/new_batch_report/full_report.csv";
  private static final String ERROR_REPORT = "/home/jochen/migration/new_batch_report/error_report.csv";

  private static final String URL_FORMAT = "https://metis-preview-portal.eanadev.org/portal/en/search?q=edm_datasetName:%s";

  // Change this: add different run IDs after each batch. Note: it is inclusive.
  private static final String FROM_RUN = MigrationResultParser.BATCH_6_FIRST_RUN_ID;
  // Change this: add different run IDs after each batch. Note: it is exclusive.
  private static final String TO_RUN = MigrationResultParser.BATCH_7_FIRST_RUN_ID;

  public static void main(String[] args) throws IOException {

    // Collect the migration model and dataset info
    final MigrationResults migrationResults = MigrationResultParser.parse(FROM_RUN, TO_RUN);
    final Map<String, MigrationResult> harvested = migrationResults
        .getMigrationResults(PluginType.OAIPMH_HARVEST);
    final Map<String, MigrationResult> indexedToPreview = migrationResults
        .getMigrationResults(PluginType.PREVIEW);
    final List<String> processedDatasets = Stream
        .concat(harvested.keySet().stream(), indexedToPreview.keySet().stream()).distinct().sorted()
        .collect(Collectors.toList());

    // Write the model to the full report.
    try (
        final Writer writer = Files.newBufferedWriter(Paths.get(FULL_REPORT));
        final CSVWriter csvWriter = new CSVWriter(writer,
            CSVWriter.DEFAULT_SEPARATOR,
            CSVWriter.DEFAULT_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER,
            CSVWriter.DEFAULT_LINE_END)
    ) {
      csvWriter.writeNext(
          new String[]{"ID", "Name", "Harvest status", "Record count",
              "Failed during harvesting", "Preview status", "Preview URI"});
      for (String datasetId : processedDatasets) {
        csvWriter.writeNext(
            createFullReportLine(datasetId, harvested.get(datasetId),
                indexedToPreview.get(datasetId)));
      }
    }

    // Write the model to the error report.
    try (
        final Writer writer = Files.newBufferedWriter(Paths.get(ERROR_REPORT));
        final CSVWriter csvWriter = new CSVWriter(writer,
            CSVWriter.DEFAULT_SEPARATOR,
            CSVWriter.DEFAULT_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER,
            CSVWriter.DEFAULT_LINE_END)
    ) {
      csvWriter.writeNext(
          new String[]{"ID", "Name", "Run", "Record count", "Harvest status", "Preview status",
              "Failed records", "Comments"});
      for (String datasetId : processedDatasets) {
        final MigrationResult harvest = harvested.get(datasetId);
        final MigrationResult indexToPreview = indexedToPreview.get(datasetId);
        if ((harvest != null && !harvest.wasSuccessful())
            || (indexToPreview != null && !indexToPreview.wasSuccessful())) {
          csvWriter.writeNext(createErrorReportLine(datasetId, harvest, indexToPreview));
        }
      }
    }
  }

  private static String[] createErrorReportLine(String datasetId, MigrationResult harvest,
      MigrationResult indexToPreview) {
    final String[] result = new String[7];
    Arrays.fill(result, "");
    result[0] = datasetId;
    result[1] = harvest == null ? indexToPreview.getDatasetInfo().getNameInCsv()
        : harvest.getDatasetInfo().getNameInCsv();
    result[2] = indexToPreview != null ? indexToPreview.getRunId() : harvest.getRunId();
    result[3] = harvest == null ? indexToPreview.getProcessedRecords().toString()
        : harvest.getProcessedRecords().toString();
    if (ResultStatus.COMPLETED_WITH_ERRORS.test(harvest)) {
      result[4] = "COMPLETED_WITH_ERRORS";
      result[6] = harvest.getErrorRecords().toString();
    } else if (ResultStatus.DID_NOT_END_NORMALLY.test(harvest)) {
      result[4] = "FAILED";
    } else if (harvest != null) {
      result[4] = "SUCCEEDED";
    }
    if (ResultStatus.COMPLETED_WITH_ERRORS.test(indexToPreview)) {
      result[5] = "COMPLETED_WITH_ERRORS";
      result[6] = indexToPreview.getErrorRecords().toString();
    } else if (ResultStatus.DID_NOT_END_NORMALLY.test(indexToPreview)) {
      result[5] = "FAILED";
    } else if (indexToPreview != null) {
      result[5] = "SUCCEEDED";
    }
    return result;
  }

  private static String[] createFullReportLine(String datasetId, MigrationResult harvest,
      MigrationResult indexToPreview) throws UnsupportedEncodingException {

    // Create result and set id and name.
    final String[] result = new String[7];
    Arrays.fill(result, "");
    result[0] = datasetId;
    result[1] = harvest == null ? indexToPreview.getDatasetInfo().getNameInCsv()
        : harvest.getDatasetInfo().getNameInCsv();
    result[3] = harvest == null ? indexToPreview.getProcessedRecords().toString()
        : harvest.getProcessedRecords().toString();

    // Set harvest info
    if (harvest != null && !ResultStatus.DID_NOT_END_NORMALLY.test(harvest)) {
      // In case we have a valid result
      result[2] = ResultStatus.COMPLETED_WITH_ERRORS.test(harvest) ? "COMPLETED_WITH_ERRORS"
          : "SUCCEEDED";
      result[4] = harvest.getErrorRecords().toString();
    } else if (harvest != null) {
      // In case something went wrong
      result[2] = "FAILED";
    }

    // Set index to preview info
    if (indexToPreview != null && !ResultStatus.DID_NOT_END_NORMALLY.test(indexToPreview)) {
      result[5] = ResultStatus.COMPLETED_WITH_ERRORS.test(indexToPreview) ? "COMPLETED_WITH_ERRORS"
          : "SUCCEEDED";
      result[6] = createUrl(datasetId);
    } else if (indexToPreview != null) {
      result[5] = "FAILED";
    }

    // Done
    return result;
  }

  private static String createUrl(String datasetId) throws UnsupportedEncodingException {
    final String query = ClientUtils.escapeQueryChars(datasetId + "_") + "*";
    return String.format(URL_FORMAT, URLEncoder.encode(query, StandardCharsets.UTF_8.name()));
  }
}
