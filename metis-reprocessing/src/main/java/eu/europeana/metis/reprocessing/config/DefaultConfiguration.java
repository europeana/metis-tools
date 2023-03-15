package eu.europeana.metis.reprocessing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.web.EntityClientApiImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.utilities.IndexUtilities;
import eu.europeana.metis.reprocessing.utilities.PostProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.NormalizerStep;
import eu.europeana.normalization.model.NormalizationBatchResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

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

    private EnrichmentWorker enrichmentWorker;
    private RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
    private Normalizer normalizer = new NormalizerFactory().getNormalizer(NormalizerStep.DATES_NORMALIZER);

    public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
            throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException {
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
    }

    private Enricher getEnricher(PropertiesHolderExtension propertiesHolderExtension) throws EnrichmentException {
        final EnricherProvider enricherProvider = new EnricherProvider();
        enricherProvider.setEntityResolver(prepareClientEntityResolver(propertiesHolderExtension));
        return enricherProvider.create();
    }

    private EntityResolver prepareClientEntityResolver(PropertiesHolderExtension propertiesHolderExtension) {
        final EntityResolver entityResolver;
        //Sanity check
        if (StringUtils.isAnyBlank(propertiesHolderExtension.entityManagementUrl, propertiesHolderExtension.entityApiUrl,
                propertiesHolderExtension.entityApiKey)) {
            throw new IllegalArgumentException("Requested resolver but configuration is missing");
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
            dereferencerProvider.setEnrichmentPropertiesValues(propertiesHolderExtension.entityManagementUrl, propertiesHolderExtension.entityApiUrl, propertiesHolderExtension.entityApiKey);
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
        rdf = dateNormalization(rdf);
        return rdf;
    }

    private RDF dateNormalization(RDF rdf) {

        RDF computedRDF = rdf;
        try {
            LOGGER.info("Date normalization");
            final String rdfString = rdfConversionUtils.convertRdfToString(rdf);
            final NormalizationBatchResult result = normalizer.normalize(Collections.singletonList(rdfString));
            computedRDF = rdfConversionUtils.convertStringToRdf(result.getNormalizedRecordsInEdmXml().get(0));
        } catch (RuntimeException | SerializationException | NormalizationException e) {
            LOGGER.warn("Something went wrong during enrichment/dereference", e);
        }
        return computedRDF;
    }

    // TODO: 15/03/2023 Needs to be updated to accomodate the approach for the coming reprocessing
    private RDF reEnrichment(RDF rdf) {
        RDF computedRDF = rdf;
        try {
            enrichmentWorker.cleanupPreviousEnrichmentEntities(rdf);
            ProcessedResult<RDF> rdfProcessedResult = enrichmentWorker.process(rdf, enrichmentWorker.getSupportedModes());
            computedRDF = rdfProcessedResult.getProcessedRecord() == null ? rdf : rdfProcessedResult.getProcessedRecord();
        } catch (RuntimeException e) {
            LOGGER.warn("Something went wrong during enrichment/dereference", e);
        }
        return computedRDF;
    }
}
