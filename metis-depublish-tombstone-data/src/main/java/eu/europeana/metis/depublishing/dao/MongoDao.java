package eu.europeana.metis.depublishing.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
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
import eu.europeana.metis.depublishing.config.PropertiesHolder;
import eu.europeana.metis.depublishing.model.DatasetStatus;
import eu.europeana.metis.depublishing.model.FailedRecord;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongo Dao for mongo.
 * <p>This is where the new records will reside as well as progress information of datasets and
 * failed records, see {@link DatasetStatus} and {@link FailedRecord}</p>
 *
 */
public class MongoDao {
  public static final String ABOUT = "about";
  private static final String DATASET_ID = "datasetId";
  private static final String FAILED_URL = "failedUrl";
  private static final String SUCCESSFULLY_REPROCESSED = "successfullyReprocessed";
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDao.class);
  private static final int DEFAULT_PAGE_SIZE = 200;
  public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;
  private final MongoInitializer mongoInitializer;
  private final Datastore mongoDatastore;
  private final Datastore mongoTombstoneDatastore;
  private final PropertiesHolder propertiesHolder;

  public MongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    PAGE_SIZE = propertiesHolder.mongoPageSize;
    mongoInitializer = prepareMongoDestinationConfiguration();
    mongoDatastore = createMongoDestinationDatastore(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
    mongoTombstoneDatastore = createMongoDestinationDatastore(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoTombstoneDb);
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    final Datastore datastore = Morphia.createDatastore(mongoClient, databaseName);
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

    return datastore;
  }

  public List<FailedRecord> getNextPageOfFailedRecords(String datasetId, int nextPage) {
    Query<FailedRecord> query = mongoTombstoneDatastore.find(FailedRecord.class);
    query.filter(Filters.regex(FAILED_URL,"^/" + datasetId + "/"))
         .filter(Filters.eq(SUCCESSFULLY_REPROCESSED, false));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * MongoDao.PAGE_SIZE)
                         .limit(MongoDao.PAGE_SIZE));
  }

  public List<DatasetStatus> getAllDatasetStatuses() {
    return MorphiaUtils
        .getListOfQueryRetryable(mongoTombstoneDatastore.find(DatasetStatus.class));
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return mongoTombstoneDatastore.find(DatasetStatus.class)
                         .filter(Filters.eq(DATASET_ID, datasetId)).first();
  }

  public void deleteDatasetStatus(String datasetId) {
    mongoTombstoneDatastore.find(DatasetStatus.class).filter(Filters.eq(DATASET_ID, datasetId))
                  .delete();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoTombstoneDatastore.save(datasetStatus));
  }

  public void storeFailedRecordToDb(FailedRecord failedRecord) {
    //Will replace it if already existent
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoTombstoneDatastore.save(failedRecord));
  }

  public void deleteFailedRecordFromDb(FailedRecord failedRecord) {
    //Will replace it if already existent
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> mongoTombstoneDatastore.delete(failedRecord));
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoTombstoneDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT,"^/" + datasetId + "/"));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE));
  }

  public List<FullBeanImpl> getRecordsFromList(List<String> recordIds) {
    List<FullBeanImpl> fullBeans = new ArrayList<>();
    Query<FullBeanImpl> query = mongoTombstoneDatastore.find(FullBeanImpl.class);
    recordIds.forEach(recordId -> {
      query.filter(Filters.eq(ABOUT, recordId));
      fullBeans.add(ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::first));
    });
    return fullBeans;
  }


  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = mongoTombstoneDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT,"^/" + datasetId + "/"));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
  }

  public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
    final Query<WebResourceMetaInfoImpl> query = mongoTombstoneDatastore
        .find(WebResourceMetaInfoImpl.class);
    final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
    query.filter(Filters.eq("_id", basicObject));
    return MorphiaUtils.getListOfQueryRetryable(query);
  }
  public void close() {
    mongoInitializer.close();
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.mongoHosts,
        propertiesHolder.mongoPorts, propertiesHolder.mongoAuthenticationDb,
        propertiesHolder.mongoUsername, propertiesHolder.mongoPassword,
        propertiesHolder.mongoEnableSSL, propertiesHolder.mongoConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

}
