package eu.europeana.metis.mongo.analyzer;

import com.mongodb.DBRef;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import dev.morphia.Datastore;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This class is used to create databases based on configuration or initialize the creation of
 * indexes on fields in an already existent database. It should be ran with extreme caution.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-01
 */
@SpringBootApplication
public class MongoAnalyzerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoAnalyzerMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;
  private static final int COUNTER_CHECKPOINT = 1000;
  private static Datastore datastore;

  @SuppressWarnings("java:S3010")
  @Autowired
  public MongoAnalyzerMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    MongoAnalyzerMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    LOGGER.info("Starting creation database script");
    SpringApplication.run(MongoAnalyzerMain.class, args);

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      MongoClient mongoClient = applicationInitializer.getMongoClient();
      datastore = new EdmMongoServerImpl(mongoClient, applicationInitializer.getMongoDatabase(),
          false).getDatastore();
      analyze();
    }
    LOGGER.info("Finished database analysis script");
  }

  private static void analyze() {
    final Map<String, Map<Integer, Integer>> datasetsWithDuplicates = new HashMap<>();
    final List<String> fieldListsToCheck = Arrays.stream(RecordListFields.values())
        .map(RecordListFields::getFieldName).collect(Collectors.toList());
    // TODO: 01/10/2020 Check collections for Aggregation and EuropeanaAggregation WebResources lists
    computeDuplicatesCounters(datasetsWithDuplicates, "record", "", fieldListsToCheck);
    LOGGER.info("");
  }

  private static void computeDuplicatesCounters(
      Map<String, Map<Integer, Integer>> datasetsWithDuplicates, String collection,
      String aboutPrefix, List<String> fieldListsToCheck) {
    final Document query = new Document("about", "/2022608/AFM_AFM_W169342");
    long counter = 0;
    try (MongoCursor<Document> cursor = datastore.getDatabase().getCollection(collection)
        .find(query).cursor()) {
      LOGGER.info("Checking collection {}", collection);
      while (cursor.hasNext()) {
        final Document doc = cursor.next();

        final String about = (String) doc.get("about");
        final String datasetId = about.substring(aboutPrefix.length() + 1, about.lastIndexOf("/"));
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
        if (counter % COUNTER_CHECKPOINT == 0 && LOGGER.isInfoEnabled()) {
          LOGGER.info("Checked {} records from record {}", counter, collection);
          LOGGER.info(datasetsWithDuplicates.toString());
        }
      }
    }
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Finished check of {}", collection);
      LOGGER.info(datasetsWithDuplicates.toString());
    }
  }

  private static <T> Map<T, Integer> findDuplicates(List<T> listWithDuplicates) {
    final Map<T, Integer> duplicates = new HashMap<>();
    final Set<T> helperSet = new HashSet<>();

    for (T item : listWithDuplicates) {
      if (!helperSet.add(item)) {
        duplicates.compute(item, (k, v) -> (v == null) ? 2 : v + 1);
      }
    }
    return duplicates;
  }

  private static <T> Map<Integer, Integer> updateDuplicatesCounters(Map<T, Integer> duplicates,
      Map<Integer, Integer> duplicatesCounters) {
    duplicates.values()
        .forEach(v -> duplicatesCounters.compute(v, (k, counter) -> (counter == null) ? 1 : v + 1));
    return duplicatesCounters;

  }

}
