package eu.europeana.metis.mongo.analyzer;

import static eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder.ERROR_LOGS_MARKER;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import eu.europeana.metis.mongo.analyzer.model.DatasetAnalysis;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Analyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);
  private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd-HHmmss");
  private static final Path analysisFilePath = Paths
      .get("analysisReport-" + simpleDateFormat.format(new Date()) + ".log");

  private final Datastore datastore;
  private final long counterCheckpoint;
  private final Document testQuery;
  private static final String ABOUT_FIELD = "about";
  private static final String RECORD_ABOUT_PREFIX = "";
  private static final String AGGREGATION_ABOUT_PREFIX = "/aggregation/provider";
  private static final String EUROPEANA_AGGREGATION_ABOUT_PREFIX = "/aggregation/europeana";

  public Analyzer(final Datastore datastore, @Nullable String testQuery,
      final long counterCheckpoint) {
    this.datastore = datastore;
    this.counterCheckpoint = counterCheckpoint;
    this.testQuery = StringUtils.isBlank(testQuery) ? null : new Document(ABOUT_FIELD, testQuery);
  }

  void analyze() {
    final List<String> fieldListsToCheck = Arrays.stream(RecordListFields.values())
        .map(RecordListFields::getFieldName).collect(Collectors.toList());
    final List<String> webResourcesField = Collections.singletonList("webResources");

    computeDuplicatesCounters("record", RECORD_ABOUT_PREFIX, fieldListsToCheck);
    computeDuplicatesCounters("Aggregation", AGGREGATION_ABOUT_PREFIX, webResourcesField);
    computeDuplicatesCounters("EuropeanaAggregation", EUROPEANA_AGGREGATION_ABOUT_PREFIX,
        webResourcesField);
  }

  private void computeDuplicatesCounters(String collection, String aboutPrefix,
      List<String> fieldListsToCheck) {
    final Map<String, DatasetAnalysis> datasetIdAndDatasetAnalysis = new HashMap<>();
    final AtomicLong unexpectedAboutValueCounter = new AtomicLong(0);
    long wrongAboutValueCounter = 0;
    long counter = 0;
    //Create find query
    final FindIterable<Document> findIterable = Optional.ofNullable(testQuery)
        .map(query -> new Document(ABOUT_FIELD, aboutPrefix + query.get(ABOUT_FIELD)))
        .map(datastore.getDatabase().getCollection(collection)::find)
        .orElseGet(datastore.getDatabase().getCollection(collection)::find);

    try (MongoCursor<Document> cursor = findIterable.cursor()) {
      LOGGER.info("Analysing collection {}", collection);
      while (cursor.hasNext()) {
        final Document document = cursor.next();

        final String about = (String) document.get(ABOUT_FIELD);
        final String datasetId;
        datasetId = getDatasetId(aboutPrefix, about, unexpectedAboutValueCounter);
        if (datasetId == null) {
          wrongAboutValueCounter++;
          continue;
        }
        analyzeDocument(fieldListsToCheck, datasetIdAndDatasetAnalysis, document, datasetId);
        counter++;
        if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
          LOGGER.info("Analysed {} records from collection {}", counter, collection);
        }
      }
    }
    if (LOGGER.isInfoEnabled()) {
      createAnalysisReportLog(datasetIdAndDatasetAnalysis, unexpectedAboutValueCounter.get(),
          wrongAboutValueCounter, collection);
    }
  }

  private void analyzeDocument(List<String> fieldListsToCheck,
      Map<String, DatasetAnalysis> datasetIdAndDatasetAnalysis, Document document,
      String datasetId) {
    final List<List<DBRef>> lists = fieldListsToCheck.stream().map(document::get)
        .filter(Objects::nonNull).map(o -> (List<DBRef>) o).collect(Collectors.toList());

    AtomicBoolean containsDuplicates = new AtomicBoolean(false);
    lists.forEach(list -> {
      final Map<DBRef, Integer> duplicates = findDuplicates(list);
      if (!duplicates.isEmpty()) {
        containsDuplicates.set(true);
        datasetIdAndDatasetAnalysis.compute(datasetId,
            (k, v) -> (v == null) ? updateDuplicatesCounters(duplicates, new DatasetAnalysis(k))
                : updateDuplicatesCounters(duplicates, v));
      }
    });
    if (containsDuplicates.get()) {
      datasetIdAndDatasetAnalysis.get(datasetId).incrementTotalRecordsWithDuplicates();
    }
  }

  private String getDatasetId(String aboutPrefix, String about,
      AtomicLong unexpectedAboutValueCounter) {
    final String datasetId;
    try {
      if (about.startsWith(aboutPrefix)) {
        datasetId = about.substring(aboutPrefix.length() + 1, about.lastIndexOf("/"));
      } else {
        //Fallback to record about prefix
        unexpectedAboutValueCounter.incrementAndGet();
        datasetId = about.substring(RECORD_ABOUT_PREFIX.length() + 1, about.lastIndexOf("/"));
        LOGGER.warn(ERROR_LOGS_MARKER,
            "(provided prefix \"{}\")Unexpected about structure, about value: {}", aboutPrefix,
            about);
      }
    } catch (StringIndexOutOfBoundsException e) {
      LOGGER.warn(ERROR_LOGS_MARKER,
          "(provided prefix \"{}\")Could not parse datasetId from about: {}", aboutPrefix, about);
      return null;
    }
    return datasetId;
  }

  private <T> Map<T, Integer> findDuplicates(List<T> listWithDuplicates) {
    final Map<T, Integer> duplicates = new HashMap<>();
    final Set<T> helperSet = new HashSet<>();

    for (T item : listWithDuplicates) {
      if (!helperSet.add(item)) {
        duplicates.compute(item, (k, v) -> (v == null) ? 2 : v + 1);
      }
    }
    return duplicates;
  }

  private <T> DatasetAnalysis updateDuplicatesCounters(Map<T, Integer> duplicates,
      DatasetAnalysis datasetAnalysis) {
    duplicates.values().forEach(v -> datasetAnalysis.getDuplicatesAndQuantity()
        .compute(v, (k, counter) -> (counter == null) ? 1 : counter + 1));
    return datasetAnalysis;
  }

  private void createAnalysisReportLog(final Map<String, DatasetAnalysis> datasetsWithDuplicates,
      Long unexpectedAboutValueCounter, long wrongAboutValueCounter, String collection) {
    final StringBuilder analysisReport = new StringBuilder();
    analysisReport.append(String.format("Analysis of collection %s%n", collection));
    final AtomicInteger totalDuplicates = new AtomicInteger();
    final AtomicInteger totalRecordsWithDuplicates = new AtomicInteger();
    //Calculate totals
    datasetsWithDuplicates.forEach((key, value) -> {
      value.getDuplicatesAndQuantity()
          .forEach((countersKey, countersValue) -> totalDuplicates.addAndGet(countersValue));
      totalRecordsWithDuplicates.addAndGet(value.getTotalRecordsWithDuplicates());
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

    try (FileWriter fw = new FileWriter(analysisFilePath.toFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
      out.println(analysisReport.toString());
    } catch (IOException e) {
      LOGGER.warn("Exception occurred while writing report to file", e);
    }
    //@formatter:on
  }

}
