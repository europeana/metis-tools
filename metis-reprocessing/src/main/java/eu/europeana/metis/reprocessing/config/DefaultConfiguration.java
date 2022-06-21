package eu.europeana.metis.reprocessing.config;

import static java.lang.String.format;
import static java.util.Objects.nonNull;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.impl.EntityResolverType;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.service.PersistentEntityResolver;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.web.EntityClientApiImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.utilities.IndexUtilities;
import eu.europeana.metis.reprocessing.utilities.PostProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.Type2;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extra configuration class that is part of {@link Configuration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered internally during the
 * re-processing.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class DefaultConfiguration extends Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfiguration.class);

  private final ThrowingBiFunction<FullBeanImpl, Configuration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, Boolean, Configuration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Date, Date, Configuration> afterReprocessProcessor;
  private Set<String> idsToRelabel;

  private EnrichmentWorker enrichmentWorker;

  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;

    initializeAdditionalElements(propertiesHolderExtension);
  }

  private void initializeAdditionalElements(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException {
    enrichmentWorker = new EnrichmentWorkerImpl(getDereferencer(propertiesHolderExtension),
        getEnricher(propertiesHolderExtension));
    idsToRelabel = propertiesHolderExtension.idsToRelabel;
  }

  private Enricher getEnricher(PropertiesHolderExtension propertiesHolderExtension) throws EnrichmentException {
    final EnricherProvider enricherProvider = new EnricherProvider();
    if (propertiesHolderExtension.entityResolverType == EntityResolverType.ENTITY_CLIENT) {
      enricherProvider.setEntityResolver(prepareClientEntityResolver(propertiesHolderExtension));
    } else if (propertiesHolderExtension.entityResolverType == EntityResolverType.PERSISTENT) {
      enricherProvider.setEntityResolver(new PersistentEntityResolver(propertiesHolderExtension.enrichmentDao));
    } else {
      if (StringUtils.isNotBlank(propertiesHolderExtension.enrichmentUrl)) {
        enricherProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
      }
    }
    return enricherProvider.create();
  }

  private EntityResolver prepareClientEntityResolver(PropertiesHolderExtension propertiesHolderExtension) {
    final EntityResolver entityResolver;
    //Sanity check
    if (StringUtils.isAnyBlank(propertiesHolderExtension.entityManagementUrl, propertiesHolderExtension.entityApiUrl,
        propertiesHolderExtension.entityApiKey)) {
      throw new IllegalArgumentException(
          format("Requested %s resolver but configuration is missing", EntityResolverType.ENTITY_CLIENT));
    }
    final Properties properties = new Properties();
    properties.put("entity.management.url", propertiesHolderExtension.entityManagementUrl);
    properties.put("entity.api.url", propertiesHolderExtension.entityApiUrl);
    properties.put("entity.api.key", propertiesHolderExtension.entityApiKey);
    entityResolver = new ClientEntityResolver(new EntityClientApiImpl(new EntityClientConfiguration(properties)),
        propertiesHolderExtension.enrichmentBatchSize);
    return entityResolver;
  }

  private Dereferencer getDereferencer(PropertiesHolderExtension propertiesHolderExtension) throws DereferenceException {
    final Dereferencer dereferencer;
    if (StringUtils.isNotBlank(propertiesHolderExtension.dereferenceUrl)) {
      final DereferencerProvider dereferencerProvider = new DereferencerProvider();
      dereferencerProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
      dereferencerProvider.setDereferenceUrl(propertiesHolderExtension.dereferenceUrl);
      dereferencer = dereferencerProvider.create();
    } else {
      dereferencer = null;
    }
    return dereferencer;
  }

  @Override
  public ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  @Override
  public ThrowingTriConsumer<RDF, Boolean, Configuration> getRdfIndexer() {
    return rdfIndexer;
  }

  @Override
  public ThrowingQuadConsumer<String, Date, Date, Configuration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    //Modify this method accordingly
    final RDF enrichedRdf = reEnrichment(rdf);
    final RDF reLabel3DTypeToImage = reLabel3DTypeToImage(enrichedRdf);
    return rightsFix(reLabel3DTypeToImage);
  }

  private RDF rightsFix(RDF rdf) {
    final String about = rdf.getProvidedCHOList().stream().findFirst().map(ProvidedCHOType::getAbout).orElse(null);
    if (nonNull(about)) {
      if (about.startsWith("/2020702")) {
        rdf.getAggregationList().stream().filter(
            aggregation -> nonNull(aggregation.getRights()) && nonNull(aggregation.getRights().getResource()) &&
                aggregation.getRights().getResource().equals(
                    "http://creativecommons.org/publicdomain/mark/1.0")).forEach(aggregation ->
            aggregation.getRights().setResource("http://creativecommons.org/publicdomain/mark/1.0/"));

      } else if (about.startsWith("/364")) {
        rdf.getAggregationList().stream().filter(
            aggregation -> nonNull(aggregation.getRights()) && nonNull(aggregation.getRights().getResource()) &&
                aggregation.getRights().getResource().equals(
                    "http://rightsstatements.org/page/InC/1.0/?language=en")).forEach(aggregation ->
            aggregation.getRights().setResource("http://rightsstatements.org/page/InC/1.0/"));
      }
    }
    return rdf;
  }

  private RDF reLabel3DTypeToImage(RDF rdf) {
    final String about = rdf.getProvidedCHOList().stream().findFirst().map(ProvidedCHOType::getAbout).orElse(null);
    if (idsToRelabel.contains(about)) {
      rdf.getProxyList().stream().filter(Objects::nonNull).filter(
             proxy -> nonNull(proxy.getType()) && nonNull(proxy.getType().getType()) && proxy.getType().getType() == EdmType._3_D)
         .forEach(proxy -> {
           Type2 type = new Type2();
           type.setType(EdmType.IMAGE);
           proxy.setType(type);
         });
    }
    return rdf;
  }

  private RDF reEnrichment(RDF rdf) {
    RDF computedRDF = rdf;
    try {
      enrichmentWorker.cleanupPreviousEnrichmentEntities(rdf);
      computedRDF = enrichmentWorker.process(rdf, enrichmentWorker.getSupportedModes());
    } catch (EnrichmentException | DereferenceException e) {
      LOGGER.warn("Something went wrong during enrichment/dereference", e);
    }
    return computedRDF;
  }
}
