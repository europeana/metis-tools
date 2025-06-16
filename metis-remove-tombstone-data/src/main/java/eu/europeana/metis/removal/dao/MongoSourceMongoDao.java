package eu.europeana.metis.removal.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.aggregation.expressions.AccumulatorExpressions;
import dev.morphia.aggregation.expressions.ArrayExpressions;
import dev.morphia.aggregation.expressions.Expressions;
import dev.morphia.aggregation.expressions.StringExpressions;
import dev.morphia.aggregation.stages.Group;
import dev.morphia.aggregation.stages.Sort;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
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
import eu.europeana.metis.removal.config.PropertiesHolder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mongo Dao for source records.
 * <p>Contains functionality for reading the source records that are being reprocessed.</p>
 */
public class MongoSourceMongoDao {

  /**
   * The constant ABOUT.
   */
  public static final String ABOUT = "about";
  private static final int DEFAULT_PAGE_SIZE = 200;
  /**
   * The constant PAGE_SIZE.
   */
  public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;

  private final MongoInitializer sourceMongoInitializer;
  private final Datastore mongoSourceTombstoneDatastore;
  private final PropertiesHolder propertiesHolder;

  /**
   * Instantiates a new Mongo source mongo dao.
   *
   * @param propertiesHolder the properties holder
   */
  public MongoSourceMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    PAGE_SIZE = propertiesHolder.sourceMongoPageSize;
    sourceMongoInitializer = prepareMongoSourceConfiguration();
    mongoSourceTombstoneDatastore = createMongoSourceDatastore(sourceMongoInitializer.getMongoClient(),
        propertiesHolder.sourceMongoTombstoneDb);
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

  /**
   * Gets next page of records.
   *
   * @param datasetId the dataset id
   * @param nextPage the next page
   * @return the next page of records
   */
  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoSourceTombstoneDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT, "^/" + datasetId + "/"));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE));
  }

  /**
   * Gets records from list.
   *
   * @param recordIds the record ids
   * @return the records from list
   */
  public List<FullBeanImpl> getRecordsFromList(List<String> recordIds) {
    List<FullBeanImpl> fullBeans = new ArrayList<>();
    Query<FullBeanImpl> query = mongoSourceTombstoneDatastore.find(FullBeanImpl.class);
    recordIds.forEach(recordId -> {
      query.filter(Filters.eq(ABOUT, recordId));
      fullBeans.add(ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::first));
    });
    return fullBeans;
  }

  /**
   * Gets all dataset ids.
   *
   * @return the all dataset ids
   */
  public List<String> getAllDatasetIds() {
    List<GroupedDataset> datasets = mongoSourceTombstoneDatastore
        .aggregate(FullBeanImpl.class)
        .group(
            Group.group(Group.id(
                    ArrayExpressions.elementAt(
                        StringExpressions.split(Expressions.field("about"),
                            Expressions.value("/")), Expressions.value(1)
                    )
                )
            ).field("totalValue",
                AccumulatorExpressions.sum(Expressions.value(1)))
        )
        .sort(Sort.sort().descending("totalValue"))
        .execute(GroupedDataset.class)
        .toList();
    return datasets.stream().map(GroupedDataset::getDatasetId).collect(Collectors.toList());
  }

  /**
   * Gets total records for dataset.
   *
   * @param datasetId the dataset id
   * @return the total records for dataset
   */
  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = mongoSourceTombstoneDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT, "^/" + datasetId + "/"));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
  }

  /**
   * Gets technical metadata for hash codes.
   *
   * @param hashCodes the hash codes
   * @return the technical metadata for hash codes
   */
  public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
    final Query<WebResourceMetaInfoImpl> query = mongoSourceTombstoneDatastore
        .find(WebResourceMetaInfoImpl.class);
    final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
    query.filter(Filters.eq("_id", basicObject));
    return MorphiaUtils.getListOfQueryRetryable(query);
  }

  /**
   * Close.
   */
  public void close() {
    sourceMongoInitializer.close();
  }

  private MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.sourceMongoHosts,
        propertiesHolder.sourceMongoPorts, propertiesHolder.sourceMongoAuthenticationDb,
        propertiesHolder.sourceMongoUsername, propertiesHolder.sourceMongoPassword,
        propertiesHolder.sourceMongoEnableSSL, propertiesHolder.sourceMongoConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }


  /**
   * The type Grouped dataset temporary class used to calculate the amount of datasets of the tombstone database to be purged.
   */
  @Entity
  public static class GroupedDataset {

    @Id
    private String datasetId;  // This will hold the group key: the segment of the "about" string
    private int totalValue;   // total of records

    /**
     * Gets dataset id.
     *
     * @return the dataset id
     */
    public String getDatasetId() {
      return datasetId;
    }
  }
}
