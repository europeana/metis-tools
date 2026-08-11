package eu.europeana.metis.reprocessing.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Mongo Dao for source records.
 * <p>Contains functionality for reading the source records that are being reprocessed.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MongoSourceMongoDao {

  public static final String ABOUT = "about";
  public static final String CONTENT_TIER_ZERO = "http://www.europeana.eu/schemas/epf/contentTier0";
  public static final String QUALITY_ANNOTATIONS = "qualityAnnotations.body";
  public static final String TYPE = "type";
  public static final String RESOURCE_TYPE = "IMAGE";
  private static final int DEFAULT_PAGE_SIZE = 200;

  private final MongoInitializer sourceMongoInitializer;
  private final Datastore mongoSourceDatastore;
  private final PropertiesHolder propertiesHolder;
  private final int pageSize;

  public MongoSourceMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    this.pageSize = propertiesHolder.sourceMongoPageSize;
    sourceMongoInitializer = prepareMongoSourceConfiguration();
    mongoSourceDatastore = createMongoSourceDatastore(sourceMongoInitializer.getMongoClient(),
        propertiesHolder.sourceMongoDb);
  }

  public int getPageSize() {
    return pageSize;
  }

  private static Datastore createMongoSourceDatastore(MongoClient mongoClient,
      String databaseName) {
    final Datastore datastore = Morphia.createDatastore(mongoClient, databaseName);
    final Mapper mapper = datastore.getMapper();
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

  private MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.sourceMongoHosts,
        propertiesHolder.sourceMongoPorts, propertiesHolder.sourceMongoAuthenticationDb,
        propertiesHolder.sourceMongoUsername, propertiesHolder.sourceMongoPassword,
        propertiesHolder.sourceMongoEnableSSL, propertiesHolder.sourceMongoConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT, "^/" + datasetId + "/"));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * pageSize).limit(pageSize));
  }

  public List<FullBeanImpl> getRecordsFromList(List<String> recordIds) {
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    query.filter(Filters.in(ABOUT, recordIds));
    return new ArrayList<>(MorphiaUtils.getListOfQueryRetryable(query));
  }

  //  public List<FullBeanImpl> getThumbnailRecordsToProcess(String datasetId, int nextPage) {
  //    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
  //
  //    query.filter(Filters.and(Filters.eq(QUALITY_ANNOTATIONS, CONTENT_TIER_ZERO),
  //        Filters.eq(TYPE, RESOURCE_TYPE)
  //    ));
  //    query.filter(Filters.regex(ABOUT, "^/" + datasetId + "/"));
  //
  //    List<FullBeanImpl> fullBeanList = MorphiaUtils.getListOfQueryRetryable(query, new FindOptions()
  //        .skip(nextPage * PAGE_SIZE)
  //        .limit(PAGE_SIZE));
  //
  //    fullBeanList = fullBeanList.stream()
  //                               .filter(recordStage -> hasThumbnailsAndValidLicense(EdmUtils.toRDF(recordStage)))
  //                               .collect(Collectors.toList());
  //    return fullBeanList;
  //  }

  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT, "^/" + datasetId + "/"));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
  }

  public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
    final Query<WebResourceMetaInfoImpl> query = mongoSourceDatastore
        .find(WebResourceMetaInfoImpl.class);
    final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
    query.filter(Filters.eq("_id", basicObject));
    return MorphiaUtils.getListOfQueryRetryable(query);
  }

  public void close() {
    if (sourceMongoInitializer != null) {
      sourceMongoInitializer.close();
    }
  }
}
