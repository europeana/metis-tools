package eu.europeana.metis.enrichment;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.enrichment.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.enrichment.utilities.MongoOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-10-15
 */
@SpringBootApplication
public class EnrichmentPopulatorMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentPopulatorMain.class);
  private static ConfigurationPropertiesHolder configurationPropertiesHolder;
  private static final String TIMESPAN_FILE_WITH_XMLS = "/tmp/timespans_new.xml";


  @SuppressWarnings("java:S3010")
  @Autowired
  public EnrichmentPopulatorMain(ConfigurationPropertiesHolder configurationPropertiesHolder) {
    EnrichmentPopulatorMain.configurationPropertiesHolder = configurationPropertiesHolder;
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(EnrichmentPopulatorMain.class, args);
    LOGGER.info("Starting population database script");

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      final MongoClient mongoClient = applicationInitializer.getMongoClient();
      final MongoOperations mongoOperations = new MongoOperations(mongoClient,
          configurationPropertiesHolder.getMongoDb());

      LOGGER.info("Start update");
//      mongoOperations.updateIsPartOfParentField(EntityType.PLACE);
//      mongoOperations.updateIsPartOfParentField(EntityType.TIMESPAN);
      LOGGER.info("End update");
    }
    LOGGER.info("Finished population database script");
  }
}
