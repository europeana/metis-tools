package eu.europeana.metis.reprocessing.dao;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import com.mongodb.MongoClient;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private Datastore metisCoreDatastore;
  private final WorkflowExecutionDao workflowExecutionDao;
  private PropertiesHolder propertiesHolder;

  public MetisCoreMongoDao(PropertiesHolder propertiesHolder)
      throws TrustStoreConfigurationException {
    this.propertiesHolder = propertiesHolder;
    metisCoreMongoInitializer = prepareMetisCoreConfiguration();
    metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
        propertiesHolder.metisCoreMongoDb);

    final MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        metisCoreMongoInitializer.getMongoClient(), propertiesHolder.metisCoreMongoDb);
    workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
  }

  public List<String> getAllDatasetIds() {
    Query<Dataset> query = metisCoreDatastore.createQuery(Dataset.class);
    final List<Dataset> datasets = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(query::asList);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  public Dataset getDataset(String datasetId) {
    return ExternalRequestUtil
        .retryableExternalRequestConnectionReset(
            () -> metisCoreDatastore.find(Dataset.class).filter(DATASET_ID, datasetId).get());
  }

  private MongoInitializer prepareMetisCoreConfiguration()
      throws TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Append default truststore with custom truststore");
      CustomTruststoreAppender
          .appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
              propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolder.metisCoreMongoHosts,
        propertiesHolder.metisCoreMongoPorts,
        propertiesHolder.metisCoreMongoAuthenticationDb,
        propertiesHolder.metisCoreMongoUsername,
        propertiesHolder.metisCoreMongoPassword,
        propertiesHolder.metisCoreMongoEnablessl,
        propertiesHolder.metisCoreMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(Dataset.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  public WorkflowExecutionDao getWorkflowExecutionDao() {
    return workflowExecutionDao;
  }

  public void close() {
    metisCoreMongoInitializer.close();
  }

}
