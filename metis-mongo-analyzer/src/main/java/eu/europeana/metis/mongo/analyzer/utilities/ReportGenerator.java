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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerator.class);
  private static final File LOG_DIRECTORY = new File("log");
  private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd-HHmmss");
  private static final String REPORT_DATE = simpleDateFormat.format(new Date());
  public static final Path ANALYSIS_FILE_PATH = Paths.get("analysisReport-" + REPORT_DATE + ".log");
  public static final String RECORDS_WITH_DUPLICATES_SUFFIX =
      "recordsWithDuplicates-" + REPORT_DATE + ".log";

  static {
    LOG_DIRECTORY.mkdir();
  }

  private ReportGenerator() {
  }

  public static void writeToFile(Path path, String text) {
    final Path writePath = Path.of(LOG_DIRECTORY.toString(), path.toString());
    try (FileWriter fw = new FileWriter(writePath.toFile(),
        true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
      out.println(text);
    } catch (IOException e) {
      LOGGER.warn("Exception occurred while writing report to file", e);
    }
  }

  public static void createAnalysisReportLog(
      final Map<String, DatasetAnalysis> datasetsWithDuplicates, Long unexpectedAboutValueCounter,
      long wrongAboutValueCounter, String collection) {
    final StringBuilder analysisReport = new StringBuilder();
    analysisReport.append(String.format("Analysis of collection %s%n", collection));
    final AtomicInteger totalDuplicates = new AtomicInteger();
    final AtomicInteger totalRecordsWithDuplicates = new AtomicInteger();

    final StringBuilder recordAbouts = new StringBuilder();
    datasetsWithDuplicates.forEach((key, datasetAnalysis) -> {
      calculateTotals(datasetAnalysis, totalDuplicates, totalRecordsWithDuplicates);
      datasetAnalysis.getRecordAboutsWithDuplicates().stream().map(about -> String.format("%s%n", about))
          .forEach(recordAbouts::append);
    });

    //Create Report
    //@formatter:off
    analysisReport.append(String.format("==============================================================%n"));
    analysisReport.append(String.format("Unexpected about values total: %s%n", unexpectedAboutValueCounter));
    analysisReport.append(String.format("Wrong about values total: %s%n", wrongAboutValueCounter));
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
    writeToFile(Paths.get(collection+ "_" + RECORDS_WITH_DUPLICATES_SUFFIX), recordAbouts.toString());
    //@formatter:on
  }

  private static void calculateTotals(DatasetAnalysis datasetAnalysis,
      AtomicInteger totalDuplicates, AtomicInteger totalRecordsWithDuplicates) {
    datasetAnalysis.getDuplicatesAndQuantity()
        .forEach((countersKey, countersValue) -> totalDuplicates.addAndGet(countersValue));
    totalRecordsWithDuplicates.addAndGet(datasetAnalysis.getRecordAboutsWithDuplicates().size());
  }

}
