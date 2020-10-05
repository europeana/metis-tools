package eu.europeana.metis.mongo.analyzer;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This class is used to analyze, reconstruct or check(readability with morphia) the mongo record
 * database.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-01
 */
@SpringBootApplication
public class MongoAnalyzerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoAnalyzerMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;

  @SuppressWarnings("java:S3010")
  @Autowired
  public MongoAnalyzerMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    MongoAnalyzerMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MongoAnalyzerMain.class, args);
    LOGGER.info("Starting database script with mode: {}", configurationPropertiesHolder.getMode());

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      MongoClient mongoClient = applicationInitializer.getMongoClient();
      final EdmMongoServerImpl edmMongoServer = new EdmMongoServerImpl(mongoClient,
          applicationInitializer.getMongoDatabase(), false);
      final Datastore datastore = edmMongoServer.getDatastore();
      switch (configurationPropertiesHolder.getMode()) {
        case ANALYSE:
          final Analyzer analyzer = new Analyzer(datastore,
              configurationPropertiesHolder.getRecordAboutToCheck(),
              configurationPropertiesHolder.getLogCounterCheckpoint());
          analyzer.analyze();
          break;
        case RECONSTRUCT:
          final Reconstructor reconstructor = new Reconstructor(datastore,
              configurationPropertiesHolder.getRecordAboutToCheck(),
              configurationPropertiesHolder.getLogCounterCheckpoint(),
              configurationPropertiesHolder.getFilePathWithCorruptedRecords());
          reconstructor.reconstruct();
          break;
        case CHECK:
          final RecordChecker recordChecker = new RecordChecker(edmMongoServer,
              configurationPropertiesHolder.getLogCounterCheckpoint(),
              configurationPropertiesHolder.getRecordAboutToCheck(),
              configurationPropertiesHolder.getFilePathWithCorruptedRecords());
          recordChecker.check();
          break;
      }
    }
    LOGGER.info("Finished database script with mode: {}", configurationPropertiesHolder.getMode());
  }
}
