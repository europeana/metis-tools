package eu.europeana.metis.processor.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.query.FindOptions;
import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.corelib.definitions.edm.beans.FullBean;
import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.config.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.utilities.FullbeanUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoSourceDao {

    private static final int DEFAULT_PAGE_SIZE = 10;
    public static final String ABOUT = "about";
    public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;
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
        Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class);
        query.filter(Filters.regex(ABOUT).pattern("^/" + datasetId + "/"));
        return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(query::count);
    }

    public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
        Query<FullBeanImpl> query = metisSourceDatastore.find(FullBeanImpl.class);
        query.filter(Filters.regex(ABOUT).pattern("^/" + datasetId + "/"));
        List<FullBeanImpl> fullBeanList = MorphiaUtils.getListOfQueryRetryable(query,
                new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE));

        for (FullBeanImpl fullBean : fullBeanList) {
            injectWebResourceMetaInfo(fullBean);
        }
        return fullBeanList;

    }

    public void injectWebResourceMetaInfo(final FullBean fullBean) {
        Map<String, WebResource> webResourceHashCodes = fullbeanUtil.prepareWebResourceHashCodes(fullBean);
        final List<WebResourceMetaInfoImpl> webResourceMetaInfos = getTechnicalMetadataForHashCodes(new ArrayList<>(webResourceHashCodes.keySet()));
        for (WebResourceMetaInfoImpl webResourceMetaInfo : webResourceMetaInfos) {
            WebResource webResource = webResourceHashCodes.get(webResourceMetaInfo.getId());
            ((WebResourceImpl) webResource).setWebResourceMetaInfo(webResourceMetaInfo);
        }
    }

    public List<WebResourceMetaInfoImpl> getTechnicalMetadataForHashCodes(List<String> hashCodes) {
        final Query<WebResourceMetaInfoImpl> query = metisSourceDatastore.find(WebResourceMetaInfoImpl.class);
        final BasicDBObject basicObject = new BasicDBObject("$in", hashCodes);
        query.filter(Filters.eq("_id", basicObject));
        return MorphiaUtils.getListOfQueryRetryable(query);
    }
}
