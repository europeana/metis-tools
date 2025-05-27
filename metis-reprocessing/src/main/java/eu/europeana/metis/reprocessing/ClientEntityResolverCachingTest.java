package eu.europeana.metis.reprocessing;

import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver.OperationMode;
import eu.europeana.enrichment.api.external.model.EnrichmentBase;
import eu.europeana.enrichment.api.internal.ReferenceTerm;
import eu.europeana.enrichment.api.internal.ReferenceTermImpl;
import eu.europeana.entity.client.EntityApiClient;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.exception.EntityClientException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientEntityResolverCachingTest {

  private static final int MAX_ENTRIES = 100;
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientEntityResolverCachingTest.class);
  private static ClientEntityResolver entityResolver;

  public static void main(String[] args) throws MalformedURLException, EntityClientException {
    final Properties properties = new Properties();
    properties.put("entity.management.url", "check metis-config for this value");
    properties.put("entity.api.url", "check metis-config for this value");
    properties.put("token_endpoint", "check metis-config for this value");
    properties.put("grant_params", "check metis-config for this value");
    entityResolver = new ClientEntityResolver(new EntityApiClient(new EntityClientConfiguration(properties)), OperationMode.CACHED);

    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ENTRIES; i++) {
      long startReq = System.nanoTime();
      ReferenceTerm referenceTerm = getReferenceTerm();
      Map<ReferenceTerm, List<EnrichmentBase>> entity = entityResolver.resolveByUri(Set.of(referenceTerm));
      LOGGER.info("Item {}: {}ns", i, System.nanoTime() - startReq);
      entity.get(referenceTerm).forEach(enrichmentBase -> LOGGER.info("enrichmentbase {}", enrichmentBase.getAbout()));

    }
    LOGGER.info("Total {}ms", (System.currentTimeMillis() - start));

    LOGGER.info("cache:{}", entityResolver.cacheReferenceTermStats());
  }

  private static ReferenceTerm getReferenceTerm() throws MalformedURLException {

    ReferenceTerm referenceTerm;
    int randomNum = (int) (Math.random() * 1001);
    if (randomNum < 500) {
      referenceTerm = new ReferenceTermImpl(URI.create("http://data.europeana.eu/organization/1482250000004671157").toURL());
    } else {
      referenceTerm = new ReferenceTermImpl(URI.create("http://data.europeana.eu/organization/1482250000004671162").toURL());
    }
    return referenceTerm;
  }


}

