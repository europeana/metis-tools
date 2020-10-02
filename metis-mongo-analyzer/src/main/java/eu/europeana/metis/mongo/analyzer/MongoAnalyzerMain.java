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
 * This class is used to analyze the mongo record database.
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
    LOGGER.info("Starting database analysis script");
    SpringApplication.run(MongoAnalyzerMain.class, args);

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      MongoClient mongoClient = applicationInitializer.getMongoClient();
      Datastore datastore = new EdmMongoServerImpl(mongoClient,
          applicationInitializer.getMongoDatabase(), false).getDatastore();
      final Analyzer analyzer = new Analyzer(datastore, configurationPropertiesHolder.getTestQueryAbout(),
          configurationPropertiesHolder.logCounterCheckpoint);
      analyzer.analyze();
    }
    LOGGER.info("Finished database analysis script");
  }
}
