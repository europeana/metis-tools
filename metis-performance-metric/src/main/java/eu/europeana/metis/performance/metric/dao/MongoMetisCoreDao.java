package eu.europeana.metis.performance.metric.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.query.Query;
import eu.europeana.metis.core.dao.DataEvolutionUtils;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mongo Dao for functionality related to metis-core
 */
public class MongoMetisCoreDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoMetisCoreDao.class);
    private final MongoInitializer metisCoreMongoInitializer;
    private final Datastore metisCoreDatastore;
    private final PropertiesHolder propertiesHolder;
    private final WorkflowExecutionDao workflowExecutionDao;
    private final DataEvolutionUtils dataEvolutionUtils;

    public MongoMetisCoreDao(PropertiesHolder propertiesHolder)
            throws CustomTruststoreAppender.TrustStoreConfigurationException {
        this.propertiesHolder = propertiesHolder;
        metisCoreMongoInitializer = prepareMetisCoreConfiguration();
        metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
                propertiesHolder.metisCoreMongoDb);
        final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
                metisCoreMongoInitializer.getMongoClient(), propertiesHolder.metisCoreMongoDb);
        workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
        dataEvolutionUtils = new DataEvolutionUtils(workflowExecutionDao);
    }

    public List<String> getAllDatasetIds() {
        Query<Dataset> query = metisCoreDatastore.find(Dataset.class);
        final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
        return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
    }

    public WorkflowExecutionDao.ResultList<WorkflowExecutionDao.ExecutionDatasetPair> getAllWorkflowsExecutionsOverview(Date startDate, Date endDate){
        //Make start date two weeks earlier, so we can include more reports based on end date
        LocalDateTime startLocalDateTime = LocalDateTime.ofInstant(startDate.toInstant(), ZoneId.systemDefault());
        startLocalDateTime = startLocalDateTime.minusWeeks(2L);
        return workflowExecutionDao.getWorkflowExecutionsOverview(null, Set.of(PluginStatus.FINISHED),
                Set.of(PluginType.PUBLISH), Date.from(startLocalDateTime.atZone(ZoneId.systemDefault()).toInstant()), endDate, 1, 2);
    }

    public PluginWithExecutionId<? extends ExecutablePlugin> getHarvesting(PluginWithExecutionId<? extends ExecutablePlugin> pluginWithExecutionId){
        return dataEvolutionUtils.getRootAncestor(pluginWithExecutionId);
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
