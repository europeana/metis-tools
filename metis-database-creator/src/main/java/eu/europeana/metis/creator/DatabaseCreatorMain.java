package eu.europeana.metis.creator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.creator.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.mongo.RecordRedirectDao;
import java.util.Arrays;
import java.util.function.BiConsumer;
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
  private static final BiConsumer<MongoClient, String> NOOP = (p1, p2) -> {
  };
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;

  @SuppressWarnings("java:S3010")
  @Autowired
  public DatabaseCreatorMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    DatabaseCreatorMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(DatabaseCreatorMain.class, args);
    LOGGER.info("Starting creation database script");

    try (final ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder); final MongoClient mongoClient = applicationInitializer
        .getMongoClient()) {
      //Drop dbs first if requested
      if (Boolean.TRUE.equals(configurationPropertiesHolder.getDatabaseDropFirst())) {
        Arrays.stream(configurationPropertiesHolder.getMongoDb()).map(mongoClient::getDatabase)
            .forEach(MongoDatabase::drop);
      }
      //Choose type of morphia initialization
      final BiConsumer<MongoClient, String> morphiaInitializer;
      switch (configurationPropertiesHolder.getCreationDatabaseType()) {
        case RECORD_REDIRECT:
          morphiaInitializer = getRecordRedirectDatabaseInitializer();
          break;
        case METIS_CORE:
          morphiaInitializer = getMetisCoreDatabaseInitializer();
          break;
        case RECORD:
          morphiaInitializer = getRecordDatabaseInitializer();
          break;
        default:
          morphiaInitializer = NOOP;
          LOGGER.info("No creation database type supplied.");
      }
      //Create dbs and/or indexes
      Arrays.stream(configurationPropertiesHolder.getMongoDb())
          .forEach(databaseName -> morphiaInitializer.accept(mongoClient, databaseName));
    }
    LOGGER.info("Finished creation database script");
  }

  /**
   * Get the initializer for record redirect database.
   * @return the initializer for record redirect database
   */
  private static BiConsumer<MongoClient, String> getRecordRedirectDatabaseInitializer() {
    return (mongoClient, databaseName) -> new RecordRedirectDao(mongoClient, databaseName, true);
  }

  /**
   * Get the initializer for metis core database.
   * @return the initializer for metis core database
   */
  private static BiConsumer<MongoClient, String> getMetisCoreDatabaseInitializer() {
    return (mongoClient, databaseName) -> new MorphiaDatastoreProviderImpl(mongoClient,
        databaseName, true);
  }

  /**
   * Get the initializer for record database.
   * <p>The europeana database that contains the
   * {@link eu.europeana.corelib.solr.bean.impl.FullBeanImpl}s</p>
   * @return the initializer for record database
   */
  private static BiConsumer<MongoClient, String> getRecordDatabaseInitializer() {
    return (mongoClient, databaseName) -> new EdmMongoServerImpl(mongoClient, databaseName, true);
  }
}
