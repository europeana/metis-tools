package eu.europeana.metis.mongo.analyzer;

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
 * <p>
 * Mode options see {@link eu.europeana.metis.mongo.analyzer.model.Mode})
 * <ul>
 *   <li>Analyse a specific record or the whole database</li>
 *   <li>Check if a specific record or a list of records can be read using morphia and {@link EdmMongoServerImpl}</li>
 *   <li>Reconstruct affected fields from a specific record or a list of records</li>
 * </ul>
 * </p>
 *
 * </p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-01
 */
@SpringBootApplication
public class MongoOperatorMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoOperatorMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;

  @SuppressWarnings("java:S3010")
  @Autowired
  public MongoOperatorMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    MongoOperatorMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MongoOperatorMain.class, args);
    LOGGER.info("Starting database script with mode: {}", configurationPropertiesHolder.getMode());

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      chooseOperator(applicationInitializer).operate();
    }
    LOGGER.info("Finished database script with mode: {}", configurationPropertiesHolder.getMode());
  }

  private static Operator chooseOperator(ApplicationInitializer applicationInitializer) {
    final Operator operator;
    switch (configurationPropertiesHolder.getMode()) {
      case ANALYSE:
        operator = new Analyzer(applicationInitializer.getMongoClient(),
            applicationInitializer.getMongoDatabase(),
            configurationPropertiesHolder.getRecordAboutToCheck(),
            configurationPropertiesHolder.getLogCounterCheckpoint());
        break;
      case RECONSTRUCT:
        operator = new Reconstructor(applicationInitializer.getMongoClient(),
            applicationInitializer.getMongoDatabase(),
            configurationPropertiesHolder.getRecordAboutToCheck(),
            configurationPropertiesHolder.getLogCounterCheckpoint(),
            configurationPropertiesHolder.getFilePathWithCorruptedRecords());
        break;
      case CHECK:
      default:
        operator = new RecordChecker(applicationInitializer.getMongoClient(),
            applicationInitializer.getMongoDatabase(),
            configurationPropertiesHolder.getLogCounterCheckpoint(),
            configurationPropertiesHolder.getRecordAboutToCheck(),
            configurationPropertiesHolder.getFilePathWithCorruptedRecords());
        break;
    }
    return operator;
  }
}
