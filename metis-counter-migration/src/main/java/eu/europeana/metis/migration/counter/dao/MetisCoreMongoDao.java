package eu.europeana.metis.migration.counter.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.config.MorphiaConfig;
import dev.morphia.mapping.DiscriminatorFunction;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.migration.counter.config.PropertiesHolder;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * The type Metis core mongo dao.
 */
@Component
public class MetisCoreMongoDao {

  private static final String DATASET_ID = "datasetId";
  private final Datastore metisCoreDatastore;
  private final WorkflowExecutionDao workflowExecutionDao;

  /**
   * Instantiates a new Metis core mongo dao.
   *
   * @param mongoClient the mongo client
   * @param propertiesHolder the properties holder
   */
  public MetisCoreMongoDao(MongoClient mongoClient, PropertiesHolder propertiesHolder) {
    metisCoreDatastore = createMetisCoreDatastore(mongoClient, propertiesHolder.getMetisCoreMongoDb());

    final MorphiaDatastoreProviderImpl morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
        mongoClient, propertiesHolder.getMetisCoreMongoDb(), false);
    workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
  }

  private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
    MorphiaConfig morphiaConfig = MorphiaConfig.load()
                                               .discriminatorKey("className")
                                               .discriminator(DiscriminatorFunction.className())
                                               .collectionNaming(NamingStrategy.identity())
                                               .database(databaseName);
    final Datastore datastore = Morphia.createDatastore(mongoClient, morphiaConfig);
    final Mapper mapper = datastore.getMapper();
    mapper.getEntityModel(Dataset.class);
    mapper.getEntityModel(WorkflowExecution.class);
    return datastore;
  }

  /**
   * Gets all dataset ids.
   *
   * @return the all dataset ids
   */
  public List<String> getAllDatasetIds() {
    Query<Dataset> query = metisCoreDatastore.find(Dataset.class);
    final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
    return datasets.stream().map(Dataset::getDatasetId).toList();
  }

  /**
   * Gets dataset.
   *
   * @param datasetId the dataset id
   * @return the dataset
   */
  public Dataset getDataset(String datasetId) {
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(
        () -> metisCoreDatastore.find(Dataset.class)
                                .filter(Filters.eq(DATASET_ID, datasetId))
                                .first());
  }

  /**
   * Gets workflow execution dao.
   *
   * @return the workflow execution dao
   */
  public WorkflowExecutionDao getWorkflowExecutionDao() {
    return workflowExecutionDao;
  }

}
