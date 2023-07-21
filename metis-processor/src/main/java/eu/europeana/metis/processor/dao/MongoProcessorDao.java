package eu.europeana.metis.processor.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.config.mongo.MongoProcessorProperties;

import java.util.List;

public class MongoProcessorDao {

    private static final String DATASET_ID = "datasetId";

    private final MongoProcessorProperties mongoProcessorProperties;
    private final MongoClient mongoClient;
    private final Datastore metisProcessorDatastore;

    public MongoProcessorDao(MongoProcessorProperties mongoProcessorProperties) throws DataAccessConfigException {
        this.mongoProcessorProperties = mongoProcessorProperties;
        this.mongoClient = initializeMongoClient();
        this.metisProcessorDatastore = initializeDatastore();
    }

    private MongoClient initializeMongoClient() throws DataAccessConfigException {
        return new MongoClientProvider<>(mongoProcessorProperties.getMongoProcessorProperties()).createMongoClient();
    }

    private Datastore initializeDatastore() {
        final Datastore datastore = Morphia.createDatastore(mongoClient, mongoProcessorProperties.getMongoProcessorDatabase());
        final Mapper mapper = datastore.getMapper();
        mapper.map(DatasetStatus.class);
        datastore.ensureIndexes();
        return datastore;
    }

    public List<DatasetStatus> getAllDatasetStatuses() {
        return MorphiaUtils.getListOfQueryRetryable(metisProcessorDatastore.find(DatasetStatus.class));
    }

    public DatasetStatus getDatasetStatus(String datasetId) {
        return metisProcessorDatastore.find(DatasetStatus.class).filter(Filters.eq(DATASET_ID, datasetId)).first();
    }

    public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
        ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> metisProcessorDatastore.save(datasetStatus));
    }
}
