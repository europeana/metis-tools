package eu.europeana.metis.reprocessing.model;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.execution.IndexUtilities;
import eu.europeana.metis.reprocessing.execution.PostProcessUtilities;
import eu.europeana.metis.reprocessing.execution.ProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import eu.europeana.metis.schema.jibx.RDF;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;

/**
 * Extra configuration class that is part of {@link BasicConfiguration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered
 * internally during the re-processing.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class ExtraConfiguration {

  private final ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Date, Date, BasicConfiguration> afterReprocessProcessor;

  private EnrichmentWorker enrichmentWorker;

  public ExtraConfiguration(PropertiesHolder propertiesHolder)
      throws DereferenceException, EnrichmentException {
    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;

    initializeAdditionalElements(propertiesHolder);
  }

  private void initializeAdditionalElements(PropertiesHolder propertiesHolder)
      throws DereferenceException, EnrichmentException {
    final PropertiesHolderExtension propertiesHolderExtension = propertiesHolder
        .getPropertiesHolderExtension();

    Enricher enricher = null;
    Dereferencer dereferencer = null;
    if (StringUtils.isNotBlank(propertiesHolderExtension.enrichmentUrl)) {
      final EnricherProvider enricherProvider = new EnricherProvider();
      enricherProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
      enricher = enricherProvider.create();
      if (StringUtils.isNotBlank(propertiesHolderExtension.dereferenceUrl)) {
        final DereferencerProvider dereferencerProvider = new DereferencerProvider();
        dereferencerProvider.setEnrichmentUrl(propertiesHolderExtension.enrichmentUrl);
        dereferencerProvider.setDereferenceUrl(propertiesHolderExtension.dereferenceUrl);
        dereferencer = dereferencerProvider.create();
      }
    }
    enrichmentWorker = new EnrichmentWorkerImpl(dereferencer, enricher);
  }

  public ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  public ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> getRdfIndexer() {
    return rdfIndexer;
  }

  public ThrowingQuadConsumer<String, Date, Date, BasicConfiguration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  public EnrichmentWorker getEnrichmentWorker() {
    return enrichmentWorker;
  }

  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {

    R apply(T t, U u) throws ProcessingException;
  }

  @FunctionalInterface
  public interface ThrowingTriConsumer<K, V, S> {

    void accept(K k, V v, S s) throws IndexingException;
  }

  @FunctionalInterface
  public interface ThrowingQuadConsumer<K, V, S, T> {

    void accept(K k, V v, S s, T t) throws ProcessingException;
  }
}
