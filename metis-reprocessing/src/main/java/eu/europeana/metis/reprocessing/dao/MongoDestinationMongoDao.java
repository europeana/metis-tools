package eu.europeana.metis.reprocessing.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.DeleteOptions;
import dev.morphia.Morphia;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filters;
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
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import java.util.List;

/**
 * Mongo Dao for destination mongo.
 * <p>This is where the new records will reside as well as progress information of datasets and
 * failed records, see {@link DatasetStatus} and {@link FailedRecord}</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MongoDestinationMongoDao {

  private static final String DATASET_ID = "datasetId";
  private static final String FAILED_URL = "failedUrl";
  private static final String SUCCESSFULLY_REPROCESSED = "successfullyReprocessed";

  private final MongoInitializer destinationMongoInitializer;
  private final Datastore mongoDestinationDatastore;
  private final PropertiesHolder propertiesHolder;

  public MongoDestinationMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    destinationMongoInitializer = prepareMongoDestinationConfiguration();
    mongoDestinationDatastore = createMongoDestinationDatastore(
        destinationMongoInitializer.getMongoClient(), propertiesHolder.destinationMongoDb);
  }

  public List<FailedRecord> getNextPageOfFailedRecords(String datasetId, int nextPage) {
    Query<FailedRecord> query = mongoDestinationDatastore.find(FailedRecord.class);
    query.filter(Filters.regex(FAILED_URL).pattern("^/" + datasetId + "/"))
        .filter(Filters.eq(SUCCESSFULLY_REPROCESSED, false));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * MongoSourceMongoDao.PAGE_SIZE)
            .limit(MongoSourceMongoDao.PAGE_SIZE));
  }

  public List<DatasetStatus> getAllDatasetStatuses() {
    return MorphiaUtils.getListOfQueryRetryable(mongoDestinationDatastore.find(DatasetStatus.class));
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return mongoDestinationDatastore.find(DatasetStatus.class)
        .filter(Filters.eq(DATASET_ID, datasetId)).first();
  }

  public void deleteDatasetStatus(String datasetId) {
    mongoDestinationDatastore.find(DatasetStatus.class).filter(Filters.eq(DATASET_ID, datasetId))
        .delete();
  }

  public void deleteAll() {
    mongoDestinationDatastore.getDatabase().drop();
    mongoDestinationDatastore.ensureIndexes();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoDestinationDatastore.save(datasetStatus));
  }

  public void storeFailedRecordToDb(FailedRecord failedRecord) {
    //Will replace it if already existent
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoDestinationDatastore.save(failedRecord));
  }

  public void deleteAllSuccessfulReprocessedFailedRecords() {
    Query<FailedRecord> query = mongoDestinationDatastore.find(FailedRecord.class);
    query.filter(Filters.eq(SUCCESSFULLY_REPROCESSED, true));
    query.delete(new DeleteOptions().multi(true));
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.destinationMongoHosts,
        propertiesHolder.destinationMongoPorts, propertiesHolder.destinationMongoAuthenticationDb,
        propertiesHolder.destinationMongoUsername, propertiesHolder.destinationMongoPassword,
        propertiesHolder.destinationMongoEnablessl);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    final Datastore datastore = Morphia.createDatastore(mongoClient, databaseName);
    final Mapper mapper = datastore.getMapper();
    mapper.map(DatasetStatus.class);
    mapper.map(FailedRecord.class);

    mapper.map(FullBeanImpl.class);
    mapper.map(ProvidedCHOImpl.class);
    mapper.map(AgentImpl.class);
    mapper.map(AggregationImpl.class);
    mapper.map(ConceptImpl.class);
    mapper.map(ProxyImpl.class);
    mapper.map(PlaceImpl.class);
    mapper.map(TimespanImpl.class);
    mapper.map(WebResourceImpl.class);
    mapper.map(EuropeanaAggregationImpl.class);
    mapper.map(EventImpl.class);
    mapper.map(PhysicalThingImpl.class);
    mapper.map(ConceptSchemeImpl.class);
    mapper.map(BasicProxyImpl.class);
    mapper.map(WebResourceMetaInfoImpl.class);
    //Ensure indexes, to create them in destination only
    datastore.ensureIndexes();
    return datastore;
  }

  public void close() {
    destinationMongoInitializer.close();
  }

}
