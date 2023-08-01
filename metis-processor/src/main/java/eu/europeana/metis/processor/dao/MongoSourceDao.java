package eu.europeana.metis.processor.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MongoSourceDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final String ABOUT = "about";
    public static final String CONTENT_TIER_ZERO = "http://www.europeana.eu/schemas/epf/contentTier0";
    public static final String QUALITY_ANNOTATIONS = "qualityAnnotations.body";
    public static final String TYPE = "type";
    public static final String RESOURCE_TYPE = "IMAGE";
    private static final Supplier<Filter> extraFilterProvider = () ->
            Filters.and(Filters.eq(TYPE, RESOURCE_TYPE), Filters.eq(QUALITY_ANNOTATIONS, CONTENT_TIER_ZERO));

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

    private MongoClient initializeMongoClient() throws DataAccessConfigException {
        return new MongoClientProvider<>(mongoSourceProperties.getMongoSourceProperties()).createMongoClient();
    }

    private Datastore initializeDatastore() {
        RecordDao recordDao = new RecordDao(mongoClient, mongoSourceProperties.getMongoSourceDatabase());
        return recordDao.getDatastore();
    }

    public long getTotalRecordsForDataset(String datasetId) {
        Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class).disableValidation();
        query.filter(generateFilter(datasetId));
        return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
    }

    @NotNull
    private static Filter generateFilter(String datasetId) {
        Filter filter = datasetIdFilterProvider.apply(datasetId);
        Filter extraFilter = extraFilterProvider.get();
        if(extraFilter != null) {
            filter = Filters.and(filter, extraFilter);
        }
        return filter;
    }

    public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage, int recordPageSize) {
        Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class);
        query.filter(generateFilter(datasetId));
        List<FullBeanImpl> fullBeanList = MorphiaUtils.getListOfQueryRetryable(query,
                new FindOptions().skip(nextPage * recordPageSize).limit(recordPageSize));

        for (FullBeanImpl fullBean : fullBeanList) {
            Map<String, WebResource> webResourceHashCodes = fullbeanUtil.prepareWebResourceHashCodes(fullBean);
            final List<WebResourceMetaInfoImpl> webResourceMetaInfos = getTechnicalMetadataForHashCodes(new ArrayList<>(webResourceHashCodes.keySet()));
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
}
