package eu.europeana.metis.performance.metric.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mongo Dao for functionality related to metis-core
 */
public class MongoMetisCoreDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoMetisCoreDao.class);
    private static final String DATASET_ID = "datasetId";
    private final MongoInitializer metisCoreMongoInitializer;
    private final Datastore metisCoreDatastore;
    private final PropertiesHolder propertiesHolder;
    private final WorkflowExecutionDao workflowExecutionDao;

    public MongoMetisCoreDao(PropertiesHolder propertiesHolder)
            throws CustomTruststoreAppender.TrustStoreConfigurationException {
        this.propertiesHolder = propertiesHolder;
        metisCoreMongoInitializer = prepareMetisCoreConfiguration();
        metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
                propertiesHolder.metisCoreMongoDb);

        final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
                metisCoreMongoInitializer.getMongoClient(), propertiesHolder.metisCoreMongoDb);
        workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
    }

    public List<Dataset> getAllDatasetsWithinDateInterval(Date startDate, Date endDate) {
        Query<Dataset> query = metisCoreDatastore.find(Dataset.class)
                .filter(Filters.gte("createdDate", startDate))
                .filter(Filters.lte("createdDate", endDate));
        return MorphiaUtils.getListOfQueryRetryable(query);
    }

    public WorkflowExecution getLatestSuccessfulWorkflowWithDatasetId(String datasetId){
        Query<WorkflowExecution> query = metisCoreDatastore.find(WorkflowExecution.class)
                .filter(Filters.eq("datasetId", datasetId))
                .filter(Filters.eq("workflowStatus", WorkflowStatus.FINISHED));
        List<WorkflowExecution> result = MorphiaUtils.getListOfQueryRetryable(query);

        if(result.isEmpty()){
            return null;
        }
        return result.stream().sorted(Comparator.comparing(WorkflowExecution::getCreatedDate))
                .collect(Collectors.toList()).get(result.size() - 1);

    }

    private MongoInitializer prepareMetisCoreConfiguration()
            throws CustomTruststoreAppender.TrustStoreConfigurationException {
        if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
                .isNotEmpty(propertiesHolder.truststorePassword)) {
            LOGGER.info("Append default truststore with custom truststore");
            CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
                    propertiesHolder.truststorePassword);
        }
        MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.metisCoreMongoHosts,
                propertiesHolder.metisCoreMongoPorts, propertiesHolder.metisCoreMongoAuthenticationDb,
                propertiesHolder.metisCoreMongoUsername, propertiesHolder.metisCoreMongoPassword,
                propertiesHolder.metisCoreMongoEnableSSL, propertiesHolder.metisCoreConnectionPoolSize);
        mongoInitializer.initializeMongoClient();
        return mongoInitializer;
    }

    private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
        MapperOptions mapperOptions = MapperOptions.builder().discriminatorKey("className")
                .discriminator(DiscriminatorFunction.className())
                .collectionNaming(NamingStrategy.identity()).build();
        final Datastore datastore = Morphia.createDatastore(mongoClient, databaseName, mapperOptions);
        final Mapper mapper = datastore.getMapper();
        mapper.map(Dataset.class);
        mapper.map(WorkflowExecution.class);
        return datastore;
    }

    public void close() {
        metisCoreMongoInitializer.close();
    }

}
