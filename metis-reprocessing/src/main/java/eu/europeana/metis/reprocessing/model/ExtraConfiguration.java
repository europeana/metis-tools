package eu.europeana.metis.reprocessing.model;

import static java.util.Objects.nonNull;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.service.PersistentEntityResolver;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.execution.IndexUtilities;
import eu.europeana.metis.reprocessing.execution.PostProcessUtilities;
import eu.europeana.metis.reprocessing.execution.ProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.Type2;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extra configuration class that is part of {@link BasicConfiguration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered internally during the
 * re-processing.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class ExtraConfiguration extends BasicConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExtraConfiguration.class);

  private final ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Date, Date, BasicConfiguration> afterReprocessProcessor;
  private final Set<String> idsToRelabel;

  private EnrichmentWorker enrichmentWorker;

  public ExtraConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;

    initializeAdditionalElements(propertiesHolderExtension);

    //Ids to relabel
    idsToRelabel = propertiesHolderExtension.idsToRelabel;
  }

  private void initializeAdditionalElements(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException {

    Enricher enricher = null;
    Dereferencer dereferencer = null;
    final EnricherProvider enricherProvider = new EnricherProvider();
    if (propertiesHolderExtension.enrichmentDao != null) {
      enricherProvider
          .setEntityResolver(new PersistentEntityResolver(propertiesHolderExtension.enrichmentDao));
      enricher = enricherProvider.create();
    } else {
      if (StringUtils.isNotBlank(propertiesHolderExtension.enrichmentUrl)) {
        enricherProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
        enricher = enricherProvider.create();
        if (StringUtils.isNotBlank(propertiesHolderExtension.dereferenceUrl)) {
          final DereferencerProvider dereferencerProvider = new DereferencerProvider();
          dereferencerProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
          dereferencerProvider.setDereferenceUrl(propertiesHolderExtension.dereferenceUrl);
          dereferencer = dereferencerProvider.create();
        }
      }
    }
    enrichmentWorker = new EnrichmentWorkerImpl(dereferencer, enricher);
  }

  @Override
  public ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  @Override
  public ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> getRdfIndexer() {
    return rdfIndexer;
  }

  @Override
  public ThrowingQuadConsumer<String, Date, Date, BasicConfiguration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    //Modify this method accordingly
    RDF enrichedRdf = reEnrichment(rdf);
    return reLabel3DTypeToImage(enrichedRdf);
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
