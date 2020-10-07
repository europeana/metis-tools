package eu.europeana.metis.mongo.analyzer;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.mongo.analyzer.model.AboutState;
import eu.europeana.metis.mongo.analyzer.model.DatasetAnalysis;
import eu.europeana.metis.mongo.analyzer.model.DatasetIdMetadata;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import eu.europeana.metis.mongo.analyzer.utilities.ReportGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer to analyse a record or a whole database.
 */
public class Analyzer implements Operator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);
  private static final String ABOUT_FIELD = "about";
  private static final String RECORD_ABOUT_PREFIX = "";
  private static final String AGGREGATION_ABOUT_PREFIX = "/aggregation/provider";
  private static final String EUROPEANA_AGGREGATION_ABOUT_PREFIX = "/aggregation/europeana";

  private final Datastore datastore;
  private final long counterCheckpoint;
  private final String recordAboutToCheck;

  public Analyzer(MongoClient mongoClient, String databaseName, @Nullable String recordAboutToCheck,
      long counterCheckpoint) {
    final EdmMongoServerImpl edmMongoServer = new EdmMongoServerImpl(mongoClient, databaseName,
        false);
    this.datastore = edmMongoServer.getDatastore();
    this.counterCheckpoint = counterCheckpoint;
    this.recordAboutToCheck = recordAboutToCheck;
  }

  public void operate() {
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
    //Create find query
    final FindIterable<Document> findIterable = Optional.ofNullable(recordAboutToCheck)
        .filter(StringUtils::isNotBlank)
        .map(about -> new Document(ABOUT_FIELD, aboutPrefix + about))
        .map(datastore.getDatabase().getCollection(collection)::find)
        .orElseGet(datastore.getDatabase().getCollection(collection)::find);

    long counter = 0;
    List<String> missingPrefixAbouts = new ArrayList<>();
    List<String> unparsableAbouts = new ArrayList<>();
    try (MongoCursor<Document> cursor = findIterable.cursor()) {
      LOGGER.info("Analysing collection {}", collection);
      while (cursor.hasNext()) {
        final Document document = cursor.next();

        final String about = (String) document.get(ABOUT_FIELD);
        final DatasetIdMetadata datasetIdMetadata;
        datasetIdMetadata = getDatasetIdMetadata(aboutPrefix, about);
        if (datasetIdMetadata.getAboutState() == AboutState.UNPARSABLE) {
          unparsableAbouts.add(about);
        } else if (datasetIdMetadata.getAboutState() == AboutState.MISSING_PREFIX) {
          missingPrefixAbouts.add(about);
        } else {
          analyzeDocument(fieldListsToCheck, datasetIdAndDatasetAnalysis, document,
              datasetIdMetadata.getDatasetId());
        }
        counter++;
        if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
          LOGGER.info("Analysed {} records from collection {}", counter, collection);
        }
      }
    }
    LOGGER.info("Analysed {} records from collection {}", counter, collection);
    new ReportGenerator()
        .createAnalysisReport(datasetIdAndDatasetAnalysis, missingPrefixAbouts, unparsableAbouts,
            collection);
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
      final DatasetAnalysis datasetAnalysis = datasetIdAndDatasetAnalysis.get(datasetId);
      datasetAnalysis.getRecordAboutsWithDuplicates().add((String) document.get(ABOUT_FIELD));
    }
  }

  private DatasetIdMetadata getDatasetIdMetadata(String aboutPrefix, String about) {
    String datasetId = null;
    AboutState aboutState = AboutState.CORRECT;
    try {
      if (about.startsWith(aboutPrefix)) {
        datasetId = about.substring(aboutPrefix.length() + 1, about.lastIndexOf("/"));
      } else {
        //Fallback to record about prefix
        datasetId = about.substring(RECORD_ABOUT_PREFIX.length() + 1, about.lastIndexOf("/"));
        aboutState = AboutState.MISSING_PREFIX;
        LOGGER.debug("(provided prefix \"{}\")Unexpected about structure, about value: {}",
            aboutPrefix, about);
      }
    } catch (StringIndexOutOfBoundsException e) {
      aboutState = AboutState.UNPARSABLE;
      LOGGER.debug("(provided prefix \"{}\")Could not parse datasetId from about: {}", aboutPrefix,
          about);
    }
    return new DatasetIdMetadata(datasetId, about, aboutState);
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

}
