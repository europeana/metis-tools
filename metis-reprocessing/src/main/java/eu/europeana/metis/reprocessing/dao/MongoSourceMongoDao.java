package eu.europeana.metis.reprocessing.dao;

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
import eu.europeana.metis.core.workflow.OrderField;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MongoSourceMongoDao {

  private static final String ID = "_id";
  private static final int DEFAULT_PAGE_SIZE = 200;
  public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;

  private final MongoInitializer mongoSourceMongoInitializer;
  private Datastore mongoSourceDatastore;
  private PropertiesHolder propertiesHolder;

  public MongoSourceMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    PAGE_SIZE = propertiesHolder.sourceMongoPageSize;
    mongoSourceMongoInitializer = prepareMongoSourceConfiguration();
    mongoSourceDatastore = createMongoSourceDatastore(
        mongoSourceMongoInitializer.getMongoClient(), propertiesHolder.sourceMongoDb);
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

  public WebResourceMetaInfoImpl getTechnicalMetadataFromSource(String resourceUrlInMd5) {
    final Query<WebResourceMetaInfoImpl> query = mongoSourceDatastore
        .createQuery(WebResourceMetaInfoImpl.class);
    return query.field(ID).equal(resourceUrlInMd5).get();
  }

  private MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolder.sourceMongoHosts,
        propertiesHolder.sourceMongoPorts,
        propertiesHolder.sourceMongoAuthenticationDb,
        propertiesHolder.sourceMongoUsername,
        propertiesHolder.sourceMongoPassword,
        propertiesHolder.sourceMongoEnablessl, propertiesHolder.sourceMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
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

  public void close() {
    mongoSourceMongoInitializer.close();
  }
}
