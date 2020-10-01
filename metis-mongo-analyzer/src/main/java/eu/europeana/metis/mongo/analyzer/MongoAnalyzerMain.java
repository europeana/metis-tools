package eu.europeana.metis.mongo.analyzer;

import eu.europeana.metis.mongo.analyzer.utilities.ConfigurationPropertiesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

/**
 * This class is used to create databases based on configuration or initialize the creation of
 * indexes on fields in an already existent database. It should be ran with extreme caution.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-01
 */
@Configuration
@SpringBootApplication
public class MongoAnalyzerMain{

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoAnalyzerMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;

  @Autowired
  public MongoAnalyzerMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    MongoAnalyzerMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    LOGGER.info("Starting creation database script");
    SpringApplication.run(MongoAnalyzerMain.class, args);

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
    }
    LOGGER.info("Finished creation database script");
  }
}
