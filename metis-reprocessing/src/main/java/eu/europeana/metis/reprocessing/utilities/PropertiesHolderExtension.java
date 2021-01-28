package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.enrichment.service.dao.EnrichmentDao;
import java.util.Arrays;

/**
 * Extra properties class that is part of {@link PropertiesHolder}.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class PropertiesHolderExtension extends PropertiesHolder {

  public final String enrichmentUrl;
  public final String dereferenceUrl;

  //Enrichment Mongo
  public final String[] metisEnrichmentMongoHosts;
  public final int[] metisEnrichmentMongoPorts;
  public final String metisEnrichmentMongoAuthenticationDb;
  public final String metisEnrichmentMongoUsername;
  public final String metisEnrichmentMongoPassword;
  public final boolean metisEnrichmentMongoEnableSSL;
  public final String metisEnrichmentMongoDb;
  public final EnrichmentDao enrichmentDao;

  public PropertiesHolderExtension(String configurationFileName) {
    super(configurationFileName);
    enrichmentUrl = properties.getProperty("enrichment.url");
    dereferenceUrl = properties.getProperty("dereference.url");

    //Enrichment Mongo
    metisEnrichmentMongoHosts = properties.getProperty("mongo.enrichment.hosts").split(",");
    metisEnrichmentMongoPorts = Arrays
        .stream(properties.getProperty("mongo.enrichment.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    metisEnrichmentMongoAuthenticationDb = properties
        .getProperty("mongo.enrichment.authentication.db");
    metisEnrichmentMongoUsername = properties.getProperty("mongo.enrichment.username");
    metisEnrichmentMongoPassword = properties.getProperty("mongo.enrichment.password");
    metisEnrichmentMongoEnableSSL = Boolean
        .parseBoolean(properties.getProperty("mongo.enrichment.enableSSL"));
    metisEnrichmentMongoDb = properties.getProperty("mongo.enrichment.db");

    enrichmentDao = new EnrichmentDao(
        prepareMongoEnrichmentConfiguration().getMongoClient(), metisEnrichmentMongoDb);
  }

  private MongoInitializer prepareMongoEnrichmentConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(metisEnrichmentMongoHosts,
        metisEnrichmentMongoPorts, metisEnrichmentMongoAuthenticationDb,
        metisEnrichmentMongoUsername, metisEnrichmentMongoPassword, metisEnrichmentMongoEnableSSL);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }
}
