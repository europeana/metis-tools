package eu.europeana.metis.mongo.analyzer.utilities;

import eu.europeana.metis.mongo.analyzer.model.DatasetAnalysis;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerator.class);
  private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd-HHmmss");
  private static final String REPORT_DATE = simpleDateFormat.format(new Date());
  private static final File REPORT_DIRECTORY = new File("report-" + REPORT_DATE);
  public static final Path ANALYSIS_FILE_PATH = Paths.get("analysisReport-" + REPORT_DATE + ".txt");
  public static final String RECORDS_WITH_DUPLICATES_SUFFIX =
      "recordsWithDuplicates-" + REPORT_DATE + ".txt";
  public static final Path MISSING_PREFIX_ABOUT_FILE_PATH = Paths
      .get("missingPrefixAbouts-" + REPORT_DATE + ".txt");
  public static final Path UNPARSABLE_ABOUT_FILE_PATH = Paths
      .get("unparsableAbouts-" + REPORT_DATE + ".txt");

  static {
    REPORT_DIRECTORY.mkdir();
  }

  private ReportGenerator() {
  }

  public static void writeToFile(Path path, String text) {
    if (StringUtils.isNotBlank(text)) {
      final Path writePath = Path.of(REPORT_DIRECTORY.toString(), path.toString());
      try (FileWriter fw = new FileWriter(writePath.toFile(), true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(
          bw)) {
        out.println(text);
      } catch (IOException e) {
        LOGGER.warn("Exception occurred while writing report to file", e);
      }
    }
  }

  public static void createAnalysisReport(
      final Map<String, DatasetAnalysis> datasetsWithDuplicates,
      List<String> missingPrefixAbouts, List<String> unparsableAbouts, String collection) {
    final StringBuilder analysisReport = new StringBuilder();
    analysisReport.append(String.format("Analysis of collection %s%n", collection));
    final AtomicInteger totalDuplicates = new AtomicInteger();
    final AtomicInteger totalRecordsWithDuplicates = new AtomicInteger();

    final StringBuilder recordAboutsLines = new StringBuilder();
    datasetsWithDuplicates.forEach((key, datasetAnalysis) -> {
      calculateTotals(datasetAnalysis, totalDuplicates, totalRecordsWithDuplicates);
      datasetAnalysis.getRecordAboutsWithDuplicates().stream()
          .map(about -> String.format("%s%n", about)).forEach(recordAboutsLines::append);
    });
    final StringBuilder missingPrefixAboutsLines = new StringBuilder();
    missingPrefixAbouts.stream().map(about -> String.format("%s%n", about))
        .forEach(missingPrefixAboutsLines::append);
    final StringBuilder unparsableAboutsLines = new StringBuilder();
    unparsableAbouts.stream().map(about -> String.format("%s%n", about))
        .forEach(unparsableAboutsLines::append);

    //Create Report
    //@formatter:off
    analysisReport.append(String.format("==============================================================%n"));
    analysisReport.append(String.format("Missing prefix on about values total: %s%n", missingPrefixAbouts.size()));
    analysisReport.append(String.format("Unparsable about values total: %s%n", unparsableAbouts.size()));
    analysisReport.append(String.format("Records with duplicates total: %s%n", totalRecordsWithDuplicates.get()));
    analysisReport.append(String.format("Duplicate counters total: %s%n", totalDuplicates.get()));
    analysisReport.append(String.format("Duplicate counters per dataset:%n"));
    //Per dataset duplicates report
    datasetsWithDuplicates.forEach((key, value) -> {
      analysisReport.append(String.format("DatasetId -> %s:%n", key));
      value.getDuplicatesAndQuantity().forEach((countersKey, countersValue) -> analysisReport
          .append(String
              .format("References of duplicates %s - Quantity %s%n", countersKey, countersValue)));
    });
    analysisReport.append(String.format("==============================================================%n"));
    writeToFile(ANALYSIS_FILE_PATH, analysisReport.toString());
    writeToFile(Paths.get(collection+ "_" + RECORDS_WITH_DUPLICATES_SUFFIX), recordAboutsLines.toString());
    writeToFile(Paths.get(collection+ "_" + MISSING_PREFIX_ABOUT_FILE_PATH), missingPrefixAboutsLines.toString());
    writeToFile(Paths.get(collection+ "_" + UNPARSABLE_ABOUT_FILE_PATH), unparsableAboutsLines.toString());
    //@formatter:on
  }

  private static void calculateTotals(DatasetAnalysis datasetAnalysis,
      AtomicInteger totalDuplicates, AtomicInteger totalRecordsWithDuplicates) {
    datasetAnalysis.getDuplicatesAndQuantity()
        .forEach((countersKey, countersValue) -> totalDuplicates.addAndGet(countersValue));
    totalRecordsWithDuplicates.addAndGet(datasetAnalysis.getRecordAboutsWithDuplicates().size());
  }

}
