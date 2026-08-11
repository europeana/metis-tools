package eu.europeana.metis.reprocessing.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.DatastoreImpl;
import dev.morphia.DeleteOptions;
import dev.morphia.Morphia;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import eu.europeana.corelib.edm.model.metainfo.AudioMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.ImageMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.TextMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.ThreeDMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.VideoMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.derived.AttributionSnippet;
import eu.europeana.corelib.solr.entity.AddressImpl;
import eu.europeana.corelib.solr.entity.AgentImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.corelib.solr.entity.BasicProxyImpl;
import eu.europeana.corelib.solr.entity.ChangeLogImpl;
import eu.europeana.corelib.solr.entity.ConceptImpl;
import eu.europeana.corelib.solr.entity.ConceptSchemeImpl;
import eu.europeana.corelib.solr.entity.DatasetImpl;
import eu.europeana.corelib.solr.entity.EuropeanaAggregationImpl;
import eu.europeana.corelib.solr.entity.EventImpl;
import eu.europeana.corelib.solr.entity.LicenseImpl;
import eu.europeana.corelib.solr.entity.OrganizationImpl;
import eu.europeana.corelib.solr.entity.PhysicalThingImpl;
import eu.europeana.corelib.solr.entity.PlaceImpl;
import eu.europeana.corelib.solr.entity.ProvidedCHOImpl;
import eu.europeana.corelib.solr.entity.ProxyImpl;
import eu.europeana.corelib.solr.entity.QualityAnnotationImpl;
import eu.europeana.corelib.solr.entity.ServiceImpl;
import eu.europeana.corelib.solr.entity.TimespanImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.PropertiesHolder;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDestinationMongoDao.class);

  private final MongoInitializer destinationMongoInitializer;
  private final DatastoreImpl mongoDestinationDatastore;
  private final DatastoreImpl mongoDestinationTombstoneDatastore;
  private final PropertiesHolder propertiesHolder;
  private final int pageSize;

  public MongoDestinationMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    this.pageSize = propertiesHolder.sourceMongoPageSize;
    destinationMongoInitializer = prepareMongoDestinationConfiguration();
    mongoDestinationDatastore = (DatastoreImpl) createMongoDestinationDatastore(
        destinationMongoInitializer.getMongoClient(), propertiesHolder.destinationMongoDb);
    mongoDestinationTombstoneDatastore = (DatastoreImpl) createMongoDestinationDatastore(
        destinationMongoInitializer.getMongoClient(), propertiesHolder.destinationMongoTombstoneDb);
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    final DatastoreImpl datastore = (DatastoreImpl) Morphia.createDatastore(mongoClient, databaseName);
    final Mapper mapper = datastore.getMapper();
    mapper.getEntityModel(DatasetStatus.class);
    mapper.getEntityModel(FailedRecord.class);

    mapper.getEntityModel(FullBeanImpl.class);
    mapper.getEntityModel(ProvidedCHOImpl.class);
    mapper.getEntityModel(AgentImpl.class);
    mapper.getEntityModel(AddressImpl.class);
    mapper.getEntityModel(AggregationImpl.class);
    mapper.getEntityModel(OrganizationImpl.class);
    mapper.getEntityModel(ConceptImpl.class);
    mapper.getEntityModel(ProxyImpl.class);
    mapper.getEntityModel(PlaceImpl.class);
    mapper.getEntityModel(TimespanImpl.class);
    mapper.getEntityModel(WebResourceImpl.class);
    mapper.getEntityModel(EuropeanaAggregationImpl.class);
    mapper.getEntityModel(ChangeLogImpl.class);
    mapper.getEntityModel(EventImpl.class);
    mapper.getEntityModel(PhysicalThingImpl.class);
    mapper.getEntityModel(ConceptSchemeImpl.class);
    mapper.getEntityModel(BasicProxyImpl.class);
    mapper.getEntityModel(WebResourceMetaInfoImpl.class);
    mapper.getEntityModel(LicenseImpl.class);
    mapper.getEntityModel(ServiceImpl.class);
    mapper.getEntityModel(QualityAnnotationImpl.class);
    mapper.getEntityModel(AttributionSnippet.class);
    mapper.getEntityModel(DatasetImpl.class);
    mapper.getEntityModel(ImageMetaInfoImpl.class);
    mapper.getEntityModel(AudioMetaInfoImpl.class);
    mapper.getEntityModel(TextMetaInfoImpl.class);
    mapper.getEntityModel(VideoMetaInfoImpl.class);
    mapper.getEntityModel(ThreeDMetaInfoImpl.class);
    //Ensure indexes, to create them in destination only
    datastore.applyIndexes();
    return datastore;
  }

  public List<FailedRecord> getNextPageOfFailedRecords(String datasetId, int nextPage) {
    Query<FailedRecord> query = mongoDestinationDatastore.find(FailedRecord.class);
    query.filter(Filters.regex(FAILED_URL, "^/" + datasetId + "/"))
         .filter(Filters.eq(SUCCESSFULLY_REPROCESSED, false));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * pageSize)
                         .limit(pageSize));
  }

  public List<DatasetStatus> getAllDatasetStatuses() {
    return MorphiaUtils
        .getListOfQueryRetryable(mongoDestinationDatastore.find(DatasetStatus.class));
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
    mongoDestinationDatastore.applyIndexes();
  }

  public void dropTemporaryCollections() {
    mongoDestinationDatastore.getDatabase().getCollection(DatasetStatus.class.getSimpleName())
                             .drop();
    mongoDestinationDatastore.getDatabase().getCollection(FailedRecord.class.getSimpleName())
                             .drop();
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

  public void deleteFailedRecordFromDb(FailedRecord failedRecord) {
    //Will replace it if already existent
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoDestinationDatastore.delete(failedRecord));
  }

  public void deleteAllSuccessfulReprocessedFailedRecords() {
    Query<FailedRecord> query = mongoDestinationDatastore.find(FailedRecord.class);
    query.filter(Filters.eq(SUCCESSFULLY_REPROCESSED, true));
    query.delete(new DeleteOptions().multi(true));
  }

  public void close() {
    destinationMongoInitializer.close();
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.destinationMongoHosts,
        propertiesHolder.destinationMongoPorts, propertiesHolder.destinationMongoAuthenticationDb,
        propertiesHolder.destinationMongoUsername, propertiesHolder.destinationMongoPassword,
        propertiesHolder.destinationMongoEnableSSL, propertiesHolder.destinationMongoConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

}
