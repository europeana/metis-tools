package eu.europeana.metis.creator;

import com.mongodb.MongoClient;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.creator.utilities.MongoInitializer;
import eu.europeana.metis.creator.utilities.PropertiesHolder;
import eu.europeana.metis.mongo.RecordRedirectDao;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to create databases based on configuration or initialize the creation of
 * indexes on fields in an already existent database. It should be ran with extreme caution.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
public class DatabaseCreatorMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCreatorMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args)
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    LOGGER.info("Starting creation database script");
    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }

    final MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    switch (propertiesHolder.creationDatabaseType) {
      case RECORD_REDIRECT:
        initializeRecordRedirectDatabase(mongoInitializer.getMongoClient(),
            propertiesHolder.mongoDb);
        break;
      case METIS_CORE:
        initializeMetisCoreDatabase(mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
        break;
      case RECORD:
        initializeRecordDatabase(mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
        break;
      default:
        LOGGER.info("No creation database type supplied.");
    }
    mongoInitializer.close();

    LOGGER.info("Finished creation database script");
  }

  private static void initializeRecordRedirectDatabase(MongoClient mongoClient,
      String databaseName) {
    new RecordRedirectDao(mongoClient, databaseName, true);
  }

  public static void initializeMetisCoreDatabase(MongoClient mongoClient, String databaseName) {
    new MorphiaDatastoreProviderImpl(mongoClient, databaseName, true);
  }

  public static void initializeRecordDatabase(MongoClient mongoClient, String databaseName) {
    new EdmMongoServerImpl(mongoClient, databaseName, true);
  }
}
