package eu.europeana.metis.reprocessing.dao;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import com.mongodb.MongoClient;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
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
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MetisCoreMongoDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetisCoreMongoDao.class);
  private static final String DATASET_ID = "datasetId";
  private final MongoInitializer metisCoreMongoInitializer;
  private Datastore metisCoreDatastore;
  private PropertiesHolderExtension propertiesHolderExtension;

  public MetisCoreMongoDao(PropertiesHolderExtension propertiesHolderExtension)
      throws TrustStoreConfigurationException {
    this.propertiesHolderExtension = propertiesHolderExtension;
    metisCoreMongoInitializer = prepareMetisCoreConfiguration();
    metisCoreDatastore = createMetisCoreDatastore(metisCoreMongoInitializer.getMongoClient(),
        propertiesHolderExtension.metisCoreMongoDb);
  }

  public List<String> getAllDatasetIdsOrdered() {
    Query<Dataset> query = metisCoreDatastore.createQuery(Dataset.class);
    //Order by dataset id which is a String order not a number order.
    query.order(DATASET_ID);
    final List<Dataset> datasets = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(query::asList);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  private MongoInitializer prepareMetisCoreConfiguration()
      throws TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolderExtension.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolderExtension.truststorePassword)) {
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Append default truststore with custom truststore");
      CustomTruststoreAppender
          .appendCustomTrustoreToDefault(propertiesHolderExtension.truststorePath,
              propertiesHolderExtension.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.metisCoreMongoHosts,
        propertiesHolderExtension.metisCoreMongoPorts,
        propertiesHolderExtension.metisCoreMongoAuthenticationDb,
        propertiesHolderExtension.metisCoreMongoUsername,
        propertiesHolderExtension.metisCoreMongoPassword,
        propertiesHolderExtension.metisCoreMongoEnablessl,
        propertiesHolderExtension.metisCoreMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(Dataset.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  public void close() {
    metisCoreMongoInitializer.close();
  }

}
