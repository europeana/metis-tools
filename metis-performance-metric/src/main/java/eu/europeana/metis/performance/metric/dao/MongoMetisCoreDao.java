package eu.europeana.metis.performance.metric.dao;

import eu.europeana.metis.core.dao.DataEvolutionUtils;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
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
    private final PropertiesHolder propertiesHolder;
    private final WorkflowExecutionDao workflowExecutionDao;
    private final DataEvolutionUtils dataEvolutionUtils;

    public MongoMetisCoreDao(PropertiesHolder propertiesHolder)
            throws CustomTruststoreAppender.TrustStoreConfigurationException {
        this.propertiesHolder = propertiesHolder;
        metisCoreMongoInitializer = prepareMetisCoreConfiguration();
        final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
                metisCoreMongoInitializer.getMongoClient(), propertiesHolder.metisCoreMongoDb);
        workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
        dataEvolutionUtils = new DataEvolutionUtils(workflowExecutionDao);
    }

    public WorkflowExecutionDao.ResultList<WorkflowExecutionDao.ExecutionDatasetPair> getAllWorkflowsExecutionsOverview(Date startDate, Date endDate){
        List<WorkflowExecutionDao.ExecutionDatasetPair> pairsList = new ArrayList<>();
        //Make start date two weeks earlier, so we can include more reports based on end date
        final LocalDateTime startLocalDateTime = LocalDateTime.ofInstant(startDate.toInstant(), ZoneId.systemDefault())
                .minusWeeks(2L);
        int nextPage = 0;
        WorkflowExecutionDao.ResultList<WorkflowExecutionDao.ExecutionDatasetPair> resultList = workflowExecutionDao
                .getWorkflowExecutionsOverview(null, Set.of(PluginStatus.FINISHED),
                Set.of(PluginType.PUBLISH), Date.from(startLocalDateTime.atZone(ZoneId.systemDefault()).toInstant()), endDate, nextPage, 1000);

        while (CollectionUtils.isNotEmpty(resultList.getResults())){
            List<WorkflowExecutionDao.ExecutionDatasetPair> filteredResult = resultList.getResults()
                    .stream()
                    .filter(pair -> isWithinInterval(pair.getExecution().getFinishedDate(), startDate, endDate))
                    .collect(Collectors.toList());
            pairsList.addAll(filteredResult);
            nextPage++;
            resultList = workflowExecutionDao
                    .getWorkflowExecutionsOverview(null, Set.of(PluginStatus.FINISHED),
                            Set.of(PluginType.PUBLISH), Date.from(startLocalDateTime.atZone(ZoneId.systemDefault()).toInstant()), endDate, nextPage, 1000);
        }

        return new WorkflowExecutionDao.ResultList<>(pairsList, true);
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

    private boolean isWithinInterval(Date dateToCheck, Date startDate, Date endDate){
        return dateToCheck.getTime() >= startDate.getTime() && dateToCheck.getTime() <= endDate.getTime();
    }

    public void close() {
        metisCoreMongoInitializer.close();
    }

}
