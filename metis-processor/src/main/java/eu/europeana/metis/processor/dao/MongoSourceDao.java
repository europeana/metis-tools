package eu.europeana.metis.processor.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.config.mongo.MongoSourceProperties;

public class MongoSourceDao {

    public static final String ABOUT = "about";

    private final MongoSourceProperties mongoSourceProperties;
    private final MongoClient mongoClient;
    private final Datastore metisSourceDatastore;

    public MongoSourceDao(MongoSourceProperties mongoSourceProperties) throws DataAccessConfigException {
        this.mongoSourceProperties = mongoSourceProperties;
        this.mongoClient = initializeMongoClient();
        this.metisSourceDatastore = initializeDatastore();

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


}
