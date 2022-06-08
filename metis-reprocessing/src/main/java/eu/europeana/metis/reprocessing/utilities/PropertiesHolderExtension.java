package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.enrichment.api.external.impl.EntityResolverType;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Value;

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
  public final int metisEnrichmentConnectionPoolSize;

  public final int enrichmentBatchSize;

  @Value("${enrichment.entity.resolver.type:PERSISTENT}")
  public final EntityResolverType entityResolverType;

  @Value("${entity.management.url}")
  public final String entityManagementUrl;

  @Value("${entity.api.url}")
  public final String entityApiUrl;

  @Value("${entity.api.key}")
  public final String entityApiKey;

  public final EnrichmentDao enrichmentDao;

  //Set of ids to relabel
  public final Set<String> idsToRelabel;

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
    metisEnrichmentConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.enrichment.connection.pool.size"), 500);

    enrichmentBatchSize = Integer
        .parseInt(properties.getProperty("enrichment.batch.size"));
    entityResolverType = EntityResolverType.valueOf(properties.getProperty("enrichment.entity.resolver.type"));
    entityManagementUrl = properties.getProperty("entity.management.url");
    entityApiUrl = properties.getProperty("entity.api.url");
    entityApiKey = properties.getProperty("entity.api.key");

    enrichmentDao = new EnrichmentDao(
        prepareMongoEnrichmentConfiguration().getMongoClient(), metisEnrichmentMongoDb);

    //Set of ids to relabel from a file with an id per line
    try (InputStream inputStream = getClass().getResourceAsStream("/ids_to_relabel.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)))) {
      idsToRelabel = reader.lines().collect(Collectors.toSet());
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private MongoInitializer prepareMongoEnrichmentConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(metisEnrichmentMongoHosts,
        metisEnrichmentMongoPorts, metisEnrichmentMongoAuthenticationDb,
        metisEnrichmentMongoUsername, metisEnrichmentMongoPassword, metisEnrichmentMongoEnableSSL,
        metisEnrichmentConnectionPoolSize);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }
}
