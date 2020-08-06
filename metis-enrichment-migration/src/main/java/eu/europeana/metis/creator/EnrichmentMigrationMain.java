package eu.europeana.metis.creator;

import static eu.europeana.metis.utils.SonarqubeNullcheckAvoidanceUtils.performFunction;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.internal.MorphiaCursor;
import eu.europeana.corelib.solr.entity.AbstractEdmEntityImpl;
import eu.europeana.corelib.solr.entity.ContextualClassImpl;
import eu.europeana.corelib.solr.entity.OrganizationImpl;
import eu.europeana.enrichment.api.internal.AgentTermList;
import eu.europeana.enrichment.api.internal.ConceptTermList;
import eu.europeana.enrichment.api.internal.MongoTermList;
import eu.europeana.enrichment.api.internal.OrganizationTermList;
import eu.europeana.enrichment.api.internal.PlaceTermList;
import eu.europeana.enrichment.api.internal.TimespanTermList;
import eu.europeana.enrichment.utils.EntityType;
import eu.europeana.metis.creator.utilities.MongoInitializer;
import eu.europeana.metis.creator.utilities.PropertiesHolder;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
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
  private static final String ENTITY_TYPE = "entityType";
  private static final int TOTAL_ITEMS_PER_PAGE = 100;
  private static boolean exitPromptly = false;
  public static final String CONCEPT_IMPL = "ConceptImpl";
  public static final String AGENT_IMPL = "AgentImpl";
  public static final String TIMESPAN_IMPL = "TimespanImpl";
  public static final String PLACE_IMPL = "PlaceImpl";
  public static final String ORGANIZATION_IMPL = "OrganizationImpl";

  private static Datastore sourceDatastore;
  private static Datastore destinationDatastore;

  public static void main(String[] args) {
    LOGGER.info("Starting migration database script");

    final MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    final MongoClient sourceMongoClient = mongoInitializer.getSourceMongoClient();
    final MongoClient destinationMongoClient = mongoInitializer.getDestinationMongoClient();
    sourceDatastore = createSourceDatastore(sourceMongoClient, propertiesHolder.sourceMongoDb);
    destinationDatastore = createDestinationDatastore(destinationMongoClient,
        propertiesHolder.destinationMongoDb);

    LOGGER.info("--Processing Concepts--");
    migrateConcepts();
    LOGGER.info("--Processing Timespans--");
    migrateTimespans();
    LOGGER.info("--Processing Places--");
    migratePlaces();
    LOGGER.info("--Processing Agents--");
    migrateAgents();
    LOGGER.info("--Processing Organizations--");
    migrateOrganizations();

//    final MongoDatabase destinationDatabase = destinationMongoClient
//        .getDatabase(propertiesHolder.destinationMongoDb);
//    executeCommandForRenamingAddress(destinationDatabase);

    mongoInitializer.close();

    LOGGER.info("Finished database migration script");
  }

  public static void executeCommandForRenamingAddress(MongoDatabase destinationDatabase) {
    String json = "   {\n"
        + "      update: 'EnrichmentTerm',\n"
        + "      updates: [\n"
        + "         {\n"
        + "           q: {},\n"
        + "           u: {$rename:{'contextualEntity.addressInt':'contextualEntity.address'}},\n"
        + "           upsert: false,\n"
        + "           multi: true,\n"
        + "         },\n"
        + "      ],\n"
        + "   }";
    destinationDatabase.runCommand(Document.parse(json));
  }

  private static <T extends MongoTermList<S>, S extends ContextualClassImpl> EnrichmentTerm convertToNewClass(
      T termList) {
    final EnrichmentTerm enrichmentTerm = new EnrichmentTerm();
    enrichmentTerm.setId(termList.getId());
    enrichmentTerm.setCodeUri(termList.getCodeUri());
    enrichmentTerm.setParent(termList.getParent());
    enrichmentTerm.setEntityType(getEntityType(termList.getEntityType()));
    final ContextualClassImpl contextualClass = termList.getRepresentation();
    fixOrganizationImplAddress(contextualClass); //Fix internal structure of address
    enrichmentTerm.setContextualEntity(contextualClass);
    enrichmentTerm.setLabelInfos(createLabelInfoList(contextualClass));
    enrichmentTerm.setOwlSameAs(
        termList.getOwlSameAs() == null ? null : Arrays.asList(termList.getOwlSameAs()));
    enrichmentTerm.setCreated(termList.getCreated());
    enrichmentTerm.setUpdated(termList.getModified());
    return enrichmentTerm;
  }

  private static List<LabelInfo> createLabelInfoList(ContextualClassImpl contextualClass) {
    final Map<String, List<String>> prefLabel = contextualClass.getPrefLabel();
    return prefLabel.entrySet().stream().map(
        entry -> new LabelInfo(entry.getValue(),
            entry.getValue().stream().map(String::toLowerCase).collect(
                Collectors.toList()), entry.getKey())).collect(Collectors.toList());
  }

  private static void fixOrganizationImplAddress(ContextualClassImpl contextualClass) {
    if (contextualClass instanceof OrganizationImpl) {
      ((OrganizationImpl) contextualClass)
          .setAddressInt(((OrganizationImpl) contextualClass).getAddress().getAddressImpl());
      ((OrganizationImpl) contextualClass).setAddress(null);
    }
  }

  private static EntityType getEntityType(String entityTypeClassName) {
    EntityType entityType;
    switch (entityTypeClassName) {
      case CONCEPT_IMPL:
        entityType = EntityType.CONCEPT;
        break;
      case AGENT_IMPL:
        entityType = EntityType.AGENT;
        break;
      case PLACE_IMPL:
        entityType = EntityType.PLACE;
        break;
      case TIMESPAN_IMPL:
        entityType = EntityType.TIMESPAN;
        break;
      case ORGANIZATION_IMPL:
        entityType = EntityType.ORGANIZATION;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + entityTypeClassName);
    }
    return entityType;
  }

  private static void migrateConcepts() {
    final long totalItems = countOfMongoTermLists(ConceptTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, CONCEPT_IMPL)));
    LOGGER.info("Total items: {}", totalItems);
    List<ConceptTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(ConceptTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, CONCEPT_IMPL)), page);
      final List<EnrichmentTerm> enrichmentTerms = allMongoTermListsByFields.stream()
          .map(EnrichmentMigrationMain::convertToNewClass).collect(
              Collectors.toList());
      destinationDatastore.save(enrichmentTerms);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!exitPromptly && !allMongoTermListsByFields.isEmpty());
  }

  private static void migrateAgents() {
    final long totalItems = countOfMongoTermLists(AgentTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, AGENT_IMPL)));
    LOGGER.info("Total items: {}", totalItems);
    List<AgentTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(AgentTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, AGENT_IMPL)), page);
      final List<EnrichmentTerm> enrichmentTerms = allMongoTermListsByFields.stream()
          .map(EnrichmentMigrationMain::convertToNewClass).collect(
              Collectors.toList());
      destinationDatastore.save(enrichmentTerms);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!exitPromptly && !allMongoTermListsByFields.isEmpty());
  }

  private static void migrateTimespans() {
    final long totalItems = countOfMongoTermLists(TimespanTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, TIMESPAN_IMPL)));
    LOGGER.info("Total items: {}", totalItems);
    List<TimespanTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(TimespanTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, TIMESPAN_IMPL)), page);
      final List<EnrichmentTerm> enrichmentTerms = allMongoTermListsByFields.stream()
          .map(EnrichmentMigrationMain::convertToNewClass).collect(
              Collectors.toList());
      destinationDatastore.save(enrichmentTerms);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!exitPromptly && !allMongoTermListsByFields.isEmpty());
  }

  private static void migratePlaces() {
    final long totalItems = countOfMongoTermLists(PlaceTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, PLACE_IMPL)));
    LOGGER.info("Total items: {}", totalItems);
    List<PlaceTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(PlaceTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, PLACE_IMPL)), page);
      final List<EnrichmentTerm> enrichmentTerms = allMongoTermListsByFields.stream()
          .map(EnrichmentMigrationMain::convertToNewClass).collect(
              Collectors.toList());
      destinationDatastore.save(enrichmentTerms);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!exitPromptly && !allMongoTermListsByFields.isEmpty());
  }

  private static void migrateOrganizations() {
    final long totalItems = countOfMongoTermLists(OrganizationTermList.class,
        Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, ORGANIZATION_IMPL)));
    LOGGER.info("Total items: {}", totalItems);
    List<OrganizationTermList> allMongoTermListsByFields;
    int page = 0;
    int itemsCount = 0;
    do {
      allMongoTermListsByFields = getAllMongoTermListsByFields(OrganizationTermList.class,
          Collections.singletonList(new ImmutablePair<>(ENTITY_TYPE, ORGANIZATION_IMPL)), page);
      final List<EnrichmentTerm> enrichmentTerms = allMongoTermListsByFields.stream()
          .map(EnrichmentMigrationMain::convertToNewClass).collect(
              Collectors.toList());
      destinationDatastore.save(enrichmentTerms);
      itemsCount += allMongoTermListsByFields.size();
      LOGGER.info("Total items processed until now: {}/{}", itemsCount, totalItems);
      page++;
    } while (!exitPromptly && !allMongoTermListsByFields.isEmpty());
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
