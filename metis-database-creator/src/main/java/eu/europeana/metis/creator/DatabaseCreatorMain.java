package eu.europeana.metis.creator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.creator.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.mongo.RecordRedirectDao;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This class is used to create databases based on configuration or initialize the creation of
 * indexes on fields in an already existent database. It should be ran with extreme caution.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-11
 */
@SpringBootApplication
public class DatabaseCreatorMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCreatorMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;

  @SuppressWarnings("java:S3010")
  @Autowired
  public DatabaseCreatorMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    DatabaseCreatorMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(DatabaseCreatorMain.class, args);
    LOGGER.info("Starting creation database script");

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      final MongoClient mongoClient = applicationInitializer.getMongoClient();
      if (configurationPropertiesHolder.getDatabaseDropFirst()) {
        Arrays.stream(configurationPropertiesHolder.getMongoDb()).map(mongoClient::getDatabase)
            .forEach(MongoDatabase::drop);
      }
      switch (configurationPropertiesHolder.getCreationDatabaseType()) {
        case RECORD_REDIRECT:
          initializeRecordRedirectDatabase(mongoClient, configurationPropertiesHolder.getMongoDb());
          break;
        case METIS_CORE:
          initializeMetisCoreDatabase(mongoClient, configurationPropertiesHolder.getMongoDb());
          break;
        case RECORD:
          initializeRecordDatabase(mongoClient, configurationPropertiesHolder.getMongoDb());
          break;
        default:
          LOGGER.info("No creation database type supplied.");
      }
    }
    LOGGER.info("Finished creation database script");
  }

  private static void initializeRecordRedirectDatabase(MongoClient mongoClient,
      String[] databaseNames) {
    Arrays.stream(databaseNames).forEach(databaseName -> {
      new RecordRedirectDao(mongoClient, databaseName, true);
    });
  }

  private static void initializeMetisCoreDatabase(MongoClient mongoClient, String[] databaseNames) {
    Arrays.stream(databaseNames).forEach(databaseName -> {
      new MorphiaDatastoreProviderImpl(mongoClient, databaseName, true);
    });
  }

  private static void initializeRecordDatabase(MongoClient mongoClient, String[] databaseNames) {
    Arrays.stream(databaseNames).forEach(databaseName -> {
      new EdmMongoServerImpl(mongoClient, databaseName, true);
    });
  }
}
