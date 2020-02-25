package eu.europeana.metis.redirects;

import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.corelib.tools.lookuptable.EuropeanaId;
import eu.europeana.metis.redirects.utilities.MongoInitializer;
import eu.europeana.metis.redirects.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-02-25
 */
public class RedirectsMigrationMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedirectsMigrationMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting redirects migration");

    final MongoInitializer mongoInitializer = prepareConfiguration();
//    final MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProviderImpl(
//        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
    final EdmMongoServerImpl europeanaId = new EdmMongoServerImpl(mongoInitializer.getMongoClient(),
        propertiesHolder.mongoDb, false);
    final Query<EuropeanaId> query = europeanaId.getDatastore().createQuery(EuropeanaId.class);
    final List<EuropeanaId> europeanaIds = query.asList();


//    final ExecutorManager executorManager = new ExecutorManager(morphiaDatastoreProvider);

    mongoInitializer.close();

  }

  private static MongoInitializer prepareConfiguration()
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

}
