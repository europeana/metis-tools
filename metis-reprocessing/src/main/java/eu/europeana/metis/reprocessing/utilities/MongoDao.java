package eu.europeana.metis.reprocessing.utilities;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import com.mongodb.MongoClient;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.AgentImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.corelib.solr.entity.BasicProxyImpl;
import eu.europeana.corelib.solr.entity.ConceptImpl;
import eu.europeana.corelib.solr.entity.ConceptSchemeImpl;
import eu.europeana.corelib.solr.entity.EuropeanaAggregationImpl;
import eu.europeana.corelib.solr.entity.EventImpl;
import eu.europeana.corelib.solr.entity.PhysicalThingImpl;
import eu.europeana.corelib.solr.entity.PlaceImpl;
import eu.europeana.corelib.solr.entity.ProvidedCHOImpl;
import eu.europeana.corelib.solr.entity.ProxyImpl;
import eu.europeana.corelib.solr.entity.TimespanImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.OrderField;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongo functionality required for the current script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MongoDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDao.class);
  private static final String ID = "_id";
  private static final String DATASET_ID = "datasetId";
  public static final int PAGE_SIZE = 200;

  private final MongoInitializer metisCoreMongoInitializer;
  private final MongoInitializer mongoSourceMongoInitializer;
  private final MongoInitializer mongoDestinationMongoInitializer;

  private Datastore metisCoreDatastore;
  private Datastore mongoSourceDatastore;
  private Datastore mongoDestinationDatastore;
  private PropertiesHolderExtension propertiesHolderExtension;

  public MongoDao(PropertiesHolderExtension propertiesHolderExtension)
      throws TrustStoreConfigurationException {
    this.propertiesHolderExtension = propertiesHolderExtension;

    //Metis Core Datastore
    metisCoreMongoInitializer = prepareMetisCoreConfiguration();
    metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
        propertiesHolderExtension.metisCoreMongoDb);

    //Mongo Source
    mongoSourceMongoInitializer = prepareMongoSourceConfiguration();
    mongoSourceDatastore = createMongoSourceDatastore(
        mongoSourceMongoInitializer.getMongoClient(), propertiesHolderExtension.sourceMongoDb);

    //Mongo Destination
    mongoDestinationMongoInitializer = prepareMongoDestinationConfiguration();
    mongoDestinationDatastore = createMongoDestinationDatastore(
        mongoDestinationMongoInitializer.getMongoClient(), propertiesHolderExtension.sourceMongoDb);
  }

  public List<String> getAllDatasetIdsOrdered() {
    Query<Dataset> query = metisCoreDatastore.createQuery(Dataset.class);
    //Order by dataset id which is a String order not a number order.
    query.order(DATASET_ID);
    final List<Dataset> datasets = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(query::asList);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoSourceDatastore.createQuery(FullBeanImpl.class);
    query.field("about").startsWith("/" + datasetId + "/");
    query.order(OrderField.ID.getOrderFieldName());
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(() -> query.asList(
        new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE)));
  }

  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = mongoSourceDatastore.createQuery(FullBeanImpl.class);
    query.field("about").startsWith("/" + datasetId + "/");
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(query::count);
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return mongoDestinationDatastore.find(DatasetStatus.class).filter(DATASET_ID, datasetId).get();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    mongoDestinationDatastore.save(datasetStatus);
  }

  public WebResourceMetaInfoImpl getTechnicalMetadataFromSource(String resourceUrlInMd5) {
    final Query<WebResourceMetaInfoImpl> query = mongoSourceDatastore
        .createQuery(WebResourceMetaInfoImpl.class);
    return query.field(ID).equal(resourceUrlInMd5).get();
  }

  private MongoInitializer prepareMetisCoreConfiguration()
      throws TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolderExtension.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolderExtension.truststorePassword)) {
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Append default truststore with custom truststore");
      CustomTruststoreAppender
          .appendCustomTrustoreToDefault(propertiesHolderExtension.truststorePath,
              propertiesHolderExtension.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.metisCoreMongoHosts,
        propertiesHolderExtension.metisCoreMongoPorts,
        propertiesHolderExtension.metisCoreMongoAuthenticationDb,
        propertiesHolderExtension.metisCoreMongoUsername,
        propertiesHolderExtension.metisCoreMongoPassword,
        propertiesHolderExtension.metisCoreMongoEnablessl,
        propertiesHolderExtension.metisCoreMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.sourceMongoHosts,
        propertiesHolderExtension.sourceMongoPorts,
        propertiesHolderExtension.sourceMongoAuthenticationDb,
        propertiesHolderExtension.sourceMongoUsername,
        propertiesHolderExtension.sourceMongoPassword,
        propertiesHolderExtension.sourceMongoEnablessl, propertiesHolderExtension.sourceMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.destinationMongoHosts,
        propertiesHolderExtension.destinationMongoPorts,
        propertiesHolderExtension.destinationMongoAuthenticationDb,
        propertiesHolderExtension.destinationMongoUsername,
        propertiesHolderExtension.destinationMongoPassword,
        propertiesHolderExtension.destinationMongoEnablessl,
        propertiesHolderExtension.destinationMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(Dataset.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  private static Datastore createMongoSourceDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(FullBeanImpl.class);
    morphia.map(ProvidedCHOImpl.class);
    morphia.map(AgentImpl.class);
    morphia.map(AggregationImpl.class);
    morphia.map(ConceptImpl.class);
    morphia.map(ProxyImpl.class);
    morphia.map(PlaceImpl.class);
    morphia.map(TimespanImpl.class);
    morphia.map(WebResourceImpl.class);
    morphia.map(EuropeanaAggregationImpl.class);
    morphia.map(EventImpl.class);
    morphia.map(PhysicalThingImpl.class);
    morphia.map(ConceptSchemeImpl.class);
    morphia.map(BasicProxyImpl.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(DatasetStatus.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    //Ensure indexes, to create them in destination only
    datastore.ensureIndexes();
    return datastore;
  }

  public void close() {
    metisCoreMongoInitializer.close();
    mongoSourceMongoInitializer.close();
    mongoDestinationMongoInitializer.close();
  }
}
