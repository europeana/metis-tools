package eu.europeana.metis.reprocessing.config;

import org.springframework.beans.factory.annotation.Value;

/**
 * Extra properties class that is part of {@link PropertiesHolder}.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class PropertiesHolderExtension extends PropertiesHolder {

  public final String dereferenceUrl;

  public final int enrichmentBatchSize;

  @Value("${entity.management.url}")
  public final String entityManagementUrl;

  @Value("${entity.api.url}")
  public final String entityApiUrl;

  @Value("${entity.api.key}")
  public final String entityApiKey;

  public PropertiesHolderExtension(String configurationFileName) {
    super(configurationFileName);
    dereferenceUrl = properties.getProperty("dereference.url");
    enrichmentBatchSize = Integer.parseInt(properties.getProperty("enrichment.batch.size"));
    entityManagementUrl = properties.getProperty("entity.management.url");
    entityApiUrl = properties.getProperty("entity.api.url");
    entityApiKey = properties.getProperty("entity.api.key");
  }
}
