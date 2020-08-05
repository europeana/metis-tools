package eu.europeana.metis.creator;

import static eu.europeana.metis.utils.SonarqubeNullcheckAvoidanceUtils.performFunction;

import com.mongodb.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.internal.MorphiaCursor;
import eu.europeana.corelib.solr.entity.AbstractEdmEntityImpl;
import eu.europeana.enrichment.api.internal.AgentTermList;
import eu.europeana.enrichment.api.internal.ConceptTermList;
import eu.europeana.enrichment.api.internal.MongoTermList;
import eu.europeana.enrichment.api.internal.OrganizationTermList;
import eu.europeana.enrichment.api.internal.PlaceTermList;
import eu.europeana.enrichment.api.internal.TimespanTermList;
import eu.europeana.metis.creator.utilities.MongoInitializer;
import eu.europeana.metis.creator.utilities.PropertiesHolder;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
public class EnrichmentMigrationMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentMigrationMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
  public static final String ENTITY_TYPE = "entityType";
  public static final int TOTAL_ITEMS_PER_PAGE = 100;
  private static Datastore sourceDatastore;

  public static void main(String[] args) {
    LOGGER.info("Starting migration database script");

    final MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    final MongoClient sourceMongoClient = mongoInitializer.getSourceMongoClient();
    sourceDatastore = createSourceDatastore(sourceMongoClient, propertiesHolder.sourceMongoDb);
//    LOGGER.info("--Processing Concepts--");
//    migrateConcepts();
//    LOGGER.info("--Processing Agents--");
//    migrateAgents();
//    LOGGER.info("--Processing Timespans--");
//    migrateTimespans();
//    LOGGER.info("--Processing Places--");
//    migratePlaces();
    LOGGER.info("--Processing Organizations--");
    migrateOrganizations();

    mongoInitializer.close();

    LOGGER.info("Finished creation database script");
  }

  private static void migrateConcepts() {
    final long totalItems = countOfMongoTermLists(ConceptTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "ConceptImpl")));
    LOGGER.info("Total items: {}", totalItems);
    List<ConceptTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(ConceptTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "ConceptImpl")), page);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!allMongoTermListsByFields.isEmpty());
  }

  private static void migrateAgents() {
    final long totalItems = countOfMongoTermLists(AgentTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "AgentImpl")));
    LOGGER.info("Total items: {}", totalItems);
    List<AgentTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(AgentTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "AgentImpl")), page);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!allMongoTermListsByFields.isEmpty());
  }

  private static void migrateTimespans() {
    final long totalItems = countOfMongoTermLists(TimespanTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "TimespanImpl")));
    LOGGER.info("Total items: {}", totalItems);
    List<TimespanTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(TimespanTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "TimespanImpl")), page);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!allMongoTermListsByFields.isEmpty());
  }

  private static void migratePlaces() {
    final long totalItems = countOfMongoTermLists(PlaceTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "PlaceImpl")));
    LOGGER.info("Total items: {}", totalItems);
    List<PlaceTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(PlaceTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "PlaceImpl")), page);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!allMongoTermListsByFields.isEmpty());
  }

  private static void migrateOrganizations() {
    final long totalItems = countOfMongoTermLists(OrganizationTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "OrganizationImpl")));
    LOGGER.info("Total items: {}", totalItems);
    List<OrganizationTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(OrganizationTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, "OrganizationImpl")), page);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!allMongoTermListsByFields.isEmpty());
  }

  private static <T extends MongoTermList<S>, S extends AbstractEdmEntityImpl> List<T> getAllMongoTermListsByFields(
      Class<T> mongoTermListType, List<Pair<String, String>> fieldNameAndValues, int nextPage) {
    final Query<T> query = sourceDatastore.createQuery(mongoTermListType);
    for (Pair<String, String> fieldNameAndValue : fieldNameAndValues) {
      query.filter(fieldNameAndValue.getKey(), fieldNameAndValue.getValue());
    }
    final FindOptions findOptions = new FindOptions().skip(nextPage * TOTAL_ITEMS_PER_PAGE)
        .limit(TOTAL_ITEMS_PER_PAGE);
    return getListOfQuery(query, findOptions);
  }

  private static <T extends MongoTermList<S>, S extends AbstractEdmEntityImpl> long countOfMongoTermLists(
      Class<T> mongoTermListType, List<Pair<String, String>> fieldNameAndValues) {
    final Query<T> query = sourceDatastore.createQuery(mongoTermListType);
    for (Pair<String, String> fieldNameAndValue : fieldNameAndValues) {
      query.filter(fieldNameAndValue.getKey(), fieldNameAndValue.getValue());
    }
    return query.count();
  }

  private static <T> List<T> getListOfQuery(Query<T> query, FindOptions findOptions) {
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(() -> {
      try (MorphiaCursor<T> cursor = query.find(findOptions)) {
        return performFunction(cursor, MorphiaCursor::toList);
      }
    });
  }


  private static Datastore createSourceDatastore(MongoClient mongoClient, String databaseName) {
    // Register the mappings and set up the data store.
    final Morphia morphia = new Morphia();
    morphia.map(MongoTermList.class);
    morphia.map(AgentTermList.class);
    morphia.map(ConceptTermList.class);
    morphia.map(PlaceTermList.class);
    morphia.map(TimespanTermList.class);
    morphia.map(OrganizationTermList.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  private static Datastore createDestinationDatastore(MongoClient mongoClient,
      String databaseName) {

    // Register the mappings and set up the data store.
    final Morphia morphia = new Morphia();
    morphia.map(EnrichmentTerm.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    datastore.ensureIndexes();
    return datastore;
  }
}
