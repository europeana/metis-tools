package eu.europeana.metis.mongo.analyzer;

import static eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder.ERROR_LOGS_MARKER;
import static eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder.STATISTICS_LOGS_MARKER;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Analyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);
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
    final Map<String, Map<Integer, Integer>> datasetsWithDuplicates = new HashMap<>();
    Long unexpectedAboutValueCounter = 0L;
    long wrongAboutValueCounter = 0;
    long counter = 0;
    final FindIterable<Document> findIterable = Optional.ofNullable(testQuery)
        .map(query -> new Document(ABOUT_FIELD, aboutPrefix + query.get(ABOUT_FIELD)))
        .map(datastore.getDatabase().getCollection(collection)::find)
        .orElseGet(datastore.getDatabase().getCollection(collection)::find);
    try (MongoCursor<Document> cursor = findIterable.cursor()) {
      LOGGER.info("Analysing collection {}", collection);
      while (cursor.hasNext()) {
        final Document doc = cursor.next();

        final String about = (String) doc.get(ABOUT_FIELD);
        final String datasetId;
        datasetId = getDatasetId(aboutPrefix, about, unexpectedAboutValueCounter);
        if (datasetId == null) {
          wrongAboutValueCounter++;
          continue;
        }
        final List<List<DBRef>> lists = fieldListsToCheck.stream().map(doc::get)
            .filter(Objects::nonNull).map(o -> (List<DBRef>) o).collect(Collectors.toList());

        lists.forEach(list -> {
          final Map<DBRef, Integer> duplicates = findDuplicates(list);
          if (!duplicates.isEmpty()) {
            final Map<Integer, Integer> duplicatesCounters = Optional
                .ofNullable(datasetsWithDuplicates.get(datasetId)).orElseGet(HashMap::new);
            datasetsWithDuplicates
                .put(datasetId, updateDuplicatesCounters(duplicates, duplicatesCounters));
          }
        });
        counter++;
        if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
          LOGGER.info("Analysed {} records from collection {}", counter, collection);
        }
      }
    }
    if (LOGGER.isInfoEnabled()) {
      duplicatesCountersLog(datasetsWithDuplicates, unexpectedAboutValueCounter,
          wrongAboutValueCounter, collection);
    }
  }

  private String getDatasetId(String aboutPrefix, String about, Long unexpectedAboutValueCounter) {
    final String datasetId;
    try {
      if (about.startsWith(aboutPrefix)) {
        datasetId = about.substring(aboutPrefix.length() + 1, about.lastIndexOf("/"));
      } else {
        //Fallback to record about prefix
        unexpectedAboutValueCounter++;
        datasetId = about.substring(RECORD_ABOUT_PREFIX.length() + 1, about.lastIndexOf("/"));
      }
    } catch (StringIndexOutOfBoundsException e) {
      LOGGER.info(ERROR_LOGS_MARKER,
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

  private <T> Map<Integer, Integer> updateDuplicatesCounters(Map<T, Integer> duplicates,
      Map<Integer, Integer> duplicatesCounters) {
    duplicates.values()
        .forEach(v -> duplicatesCounters.compute(v, (k, counter) -> (counter == null) ? 1 : v + 1));
    return duplicatesCounters;
  }

  private void duplicatesCountersLog(
      final Map<String, Map<Integer, Integer>> datasetsWithDuplicates,
      Long unexpectedAboutValueCounter, long wrongAboutValueCounter, String collection) {
    LOGGER.info(STATISTICS_LOGS_MARKER, "Analysis of collection {}", collection);
    final AtomicInteger totalDuplicates = new AtomicInteger();
    datasetsWithDuplicates.forEach((key, value) -> value
        .forEach((countersKey, countersValue) -> totalDuplicates.addAndGet(countersValue)));
    LOGGER.info(STATISTICS_LOGS_MARKER,
        "==============================================================");
    LOGGER.info(STATISTICS_LOGS_MARKER, "Unexpected about values total: {}",
        unexpectedAboutValueCounter);
    LOGGER.info(STATISTICS_LOGS_MARKER, "Wrong about values total: {}", wrongAboutValueCounter);
    LOGGER.info(STATISTICS_LOGS_MARKER, "Duplicate counters total: {}", totalDuplicates.get());
    LOGGER.info(STATISTICS_LOGS_MARKER, "Duplicate counters per dataset:");
    datasetsWithDuplicates.forEach((key, value) -> {
      LOGGER.info(STATISTICS_LOGS_MARKER, "DatasetId -> {}:", key);
      value.forEach((countersKey, countersValue) -> LOGGER
          .info(STATISTICS_LOGS_MARKER, "References of duplicates {} - Quantity {}", countersKey,
              countersValue));
    });
    LOGGER.info(STATISTICS_LOGS_MARKER,
        "==============================================================");
  }

}
