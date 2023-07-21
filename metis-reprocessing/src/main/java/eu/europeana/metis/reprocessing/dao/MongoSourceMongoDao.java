package eu.europeana.metis.reprocessing.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
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

  private static final int DEFAULT_PAGE_SIZE = 200;
  public static final String ABOUT = "about";
  public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;

  private final MongoInitializer sourceMongoInitializer;
  private final Datastore mongoSourceDatastore;
  // TODO: 15/05/2023 Temporary Datastore for translations
  private final Datastore mongoSourceTranslationsDatastore;
  private final PropertiesHolder propertiesHolder;

  public MongoSourceMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    PAGE_SIZE = propertiesHolder.sourceMongoPageSize;
    sourceMongoInitializer = prepareMongoSourceConfiguration();
    mongoSourceDatastore = createMongoSourceDatastore(sourceMongoInitializer.getMongoClient(),
        propertiesHolder.sourceMongoDb);
    mongoSourceTranslationsDatastore = createMongoSourceDatastore(sourceMongoInitializer.getMongoClient(),
            propertiesHolder.sourceTranslationsMongoDb);
  }

  // TODO: 15/05/2023 Temporary method for translations.
  public FullBeanImpl getTranslationsRecord(String about){
    Query<FullBeanImpl> query = mongoSourceTranslationsDatastore.find(FullBeanImpl.class);
    query.filter(Filters.eq(ABOUT, about));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::first);
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT).pattern("^/" + datasetId + "/"));
    return MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE));
  }

  public List<FullBeanImpl> getRecordsFromList(List<String> recordIds) {
    List<FullBeanImpl> fullBeans = new ArrayList<>();
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    recordIds.forEach(recordId -> {
      query.filter(Filters.eq(ABOUT, recordId));
      fullBeans.add(ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::first));
    });
    return fullBeans;
  }

  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = mongoSourceDatastore.find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT).pattern("^/" + datasetId + "/"));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
  }

  public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
    final Query<WebResourceMetaInfoImpl> query = mongoSourceDatastore
        .find(WebResourceMetaInfoImpl.class);
    final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
    query.filter(Filters.eq("_id", basicObject));
    return MorphiaUtils.getListOfQueryRetryable(query);
  }

  private MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.sourceMongoHosts,
        propertiesHolder.sourceMongoPorts, propertiesHolder.sourceMongoAuthenticationDb,
        propertiesHolder.sourceMongoUsername, propertiesHolder.sourceMongoPassword,
        propertiesHolder.sourceMongoEnableSSL, propertiesHolder.sourceMongoConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMongoSourceDatastore(MongoClient mongoClient,
      String databaseName) {
    final Datastore datastore = Morphia.createDatastore(mongoClient, databaseName);
    final Mapper mapper = datastore.getMapper();
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
    return datastore;
  }

  public void close() {
    sourceMongoInitializer.close();
  }
}
