package eu.europeana.metis.creator;

import com.mongodb.client.MongoClient;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.metis.creator.utilities.ConfigurationPropertiesHolder;
import eu.europeana.metis.creator.utilities.EnrichmentTermUtils;
import eu.europeana.metis.creator.utilities.MongoPopulator;
import java.util.List;
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

    final List<EnrichmentTerm> timespansFromDocument = EnrichmentTermUtils
        .getTimespansFromDocument(TIMESPAN_FILE_WITH_XMLS);

    try (ApplicationInitializer applicationInitializer = new ApplicationInitializer(
        configurationPropertiesHolder)) {
      final MongoClient mongoClient = applicationInitializer.getMongoClient();
      final EnrichmentDao enrichmentDao = new EnrichmentDao(mongoClient,
          configurationPropertiesHolder.getMongoDb());
      MongoPopulator.replaceSemiumWithEntities(enrichmentDao, timespansFromDocument);
    }
    LOGGER.info("Finished population database script");
  }
}
