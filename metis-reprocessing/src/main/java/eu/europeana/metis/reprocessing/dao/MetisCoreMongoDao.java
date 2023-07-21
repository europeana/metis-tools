package eu.europeana.metis.reprocessing.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Mongo Dao for functionality related to metis-core, simplified.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MetisCoreMongoDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetisCoreMongoDao.class);
  private static final String DATASET_ID = "datasetId";
  private final MongoInitializer metisCoreMongoInitializer;
  private final Datastore metisCoreDatastore;
  private final PropertiesHolder propertiesHolder;
  private final WorkflowExecutionDao workflowExecutionDao;

  public MetisCoreMongoDao(PropertiesHolder propertiesHolder)
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    this.propertiesHolder = propertiesHolder;
    metisCoreMongoInitializer = prepareMetisCoreConfiguration();
    metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
        propertiesHolder.metisCoreMongoDb);

    final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
        metisCoreMongoInitializer.getMongoClient(), propertiesHolder.metisCoreMongoDb);
    workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
  }

  public List<String> getAllDatasetIds() {
    Query<Dataset> query = metisCoreDatastore.find(Dataset.class);
    final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  public Dataset getDataset(String datasetId) {
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> metisCoreDatastore.find(Dataset.class).filter(Filters.eq(DATASET_ID, datasetId))
            .first());
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

  public WorkflowExecutionDao getWorkflowExecutionDao() {
    return workflowExecutionDao;
  }

  public void close() {
    metisCoreMongoInitializer.close();
  }

}
