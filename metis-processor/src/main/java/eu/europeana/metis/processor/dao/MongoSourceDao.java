package eu.europeana.metis.processor.dao;

import static dev.morphia.query.Sort.ascending;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.SocketSettings;
import dev.morphia.Datastore;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filter;
import dev.morphia.query.filters.Filters;
import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.properties.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.utilities.FullbeanUtil;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSourceDao {

  public static final String ABOUT = "about";
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Supplier<Filter> extraFilterProvider = () -> null;

  /**
   * Change this to add an extra filter or return null to not apply any additional filters
   */
  private static final Function<String, Filter> datasetIdFilterProvider =
      datasetId -> Filters.regex(ABOUT).pattern("^/" + datasetId + "/");
  private final MongoSourceProperties mongoSourceProperties;
  private final MongoClient mongoClient;
  private final Datastore metisSourceDatastore;
  private final FullbeanUtil fullbeanUtil;

  public MongoSourceDao(MongoSourceProperties mongoSourceProperties) throws DataAccessConfigException {
    this.mongoSourceProperties = mongoSourceProperties;
    this.mongoClient = initializeMongoClient();
    this.metisSourceDatastore = initializeDatastore();
    this.fullbeanUtil = new FullbeanUtil();
  }

  @NotNull
  private static Filter generateFilter(String datasetId) {
    Filter filter = datasetIdFilterProvider.apply(datasetId);
    Filter extraFilter = extraFilterProvider.get();
    if (extraFilter != null) {
      filter = Filters.and(filter, extraFilter);
    }
    return filter;
  }

  private static Filter generateFilter(String datasetId, ObjectId nextPageId) {
    Filter filter = datasetIdFilterProvider.apply(datasetId);
    Filter extraFilter = extraFilterProvider.get();
    if (extraFilter != null) {
      filter = Filters.and(filter, extraFilter);
    }
    if (nextPageId != null) {
      filter = Filters.and(Filters.gt("_id", nextPageId), filter);
    }
    return filter;
  }

  public long getTotalRecordsForDataset(String datasetId) {
    Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class).disableValidation();
    query.filter(generateFilter(datasetId));
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, ObjectId nextPageId, int recordPageSize) {
    Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class);
    query.filter(generateFilter(datasetId, nextPageId));
    List<FullBeanImpl> fullBeanList = MorphiaUtils.getListOfQueryRetryable(query,
        new FindOptions().limit(recordPageSize).sort(ascending("_id")));

    for (FullBeanImpl fullBean : fullBeanList) {
      Map<String, WebResource> webResourceHashCodes = fullbeanUtil.prepareWebResourceHashCodes(fullBean);
      final List<WebResourceMetaInfoImpl> webResourceMetaInfos = getTechnicalMetadataForHashCodes(
          new ArrayList<>(webResourceHashCodes.keySet()));
      fullbeanUtil.injectWebResourceMetaInfo(webResourceHashCodes, webResourceMetaInfos);
    }
    return fullBeanList;

  }

  public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
    final Query<WebResourceMetaInfoImpl> query = metisSourceDatastore.find(WebResourceMetaInfoImpl.class);
    final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
    query.filter(Filters.eq("_id", basicObject));
    return MorphiaUtils.getListOfQueryRetryable(query);
  }

  private MongoClient initializeMongoClient() throws DataAccessConfigException {
    LOGGER.info("Initializing mongo client");
    // Configure SocketSettings with a 16 MB buffer for send and receive
    SocketSettings socketSettings = SocketSettings.builder()
                                                  .receiveBufferSize(16 * 1024 * 1024) // 16 MB buffer for receiving
                                                  .sendBufferSize(16 * 1024 * 1024)    // 16 MB buffer for sending
                                                  .connectTimeout(30, TimeUnit.SECONDS) // Connection timeout
                                                  .readTimeout(30, TimeUnit.SECONDS)    // Read timeout
                                                  .build();
    MongoClientSettings.Builder settings = MongoClientSettings.builder()
                                                              .applyToSocketSettings(
                                                                  builder -> builder.applySettings(socketSettings))
                                                              .compressorList(
                                                                  Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                                                      MongoCompressor.createZlibCompressor(),
                                                                      MongoCompressor.createZstdCompressor()));
    return new MongoClientProvider<>(mongoSourceProperties.getMongoSourceProperties(), settings).createMongoClient();
  }

  //    public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage, int recordPageSize) {
  //        Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class);
  //        query.filter(generateFilter(datasetId));
  //        List<FullBeanImpl> fullBeanList = MorphiaUtils.getListOfQueryRetryable(query,
  //                new FindOptions().skip(nextPage * recordPageSize).limit(recordPageSize));
  //
  //        for (FullBeanImpl fullBean : fullBeanList) {
  //            Map<String, WebResource> webResourceHashCodes = fullbeanUtil.prepareWebResourceHashCodes(fullBean);
  //            final List<WebResourceMetaInfoImpl> webResourceMetaInfos = getTechnicalMetadataForHashCodes(new ArrayList<>(webResourceHashCodes.keySet()));
  //            fullbeanUtil.injectWebResourceMetaInfo(webResourceHashCodes, webResourceMetaInfos);
  //        }
  //        return fullBeanList;
  //
  //    }

  private Datastore initializeDatastore() {
    LOGGER.info("Initializing datastore");
    RecordDao recordDao = new RecordDao(mongoClient, mongoSourceProperties.getMongoSourceDatabase());
    return recordDao.getDatastore();
  }
}
