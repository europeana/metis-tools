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
  private static final String TIMESPAN_FILE_WITH_XMLS = "/tmp/timespans.xml";


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
      //Fix className in Organization Address
      mongoOperations.updateClassNameInAddress();
      LOGGER.info("Fixed address fields");

      //Get timespans from file and update the matching ones in the database
      mongoOperations.updateTimespanEntitiesFromFile(TIMESPAN_FILE_WITH_XMLS);
      LOGGER.info("Updated timespans");

      //Update all EnrichmentTerms fields
      LOGGER.info("Starting update of EnrichmentTerms fields, it will take a few minutes");
      mongoOperations.updateEnrichmentTermsFields();
      LOGGER.info("Updated all EnrichmentTerms fields");
    }
    LOGGER.info("Finished population database script");
  }
}
