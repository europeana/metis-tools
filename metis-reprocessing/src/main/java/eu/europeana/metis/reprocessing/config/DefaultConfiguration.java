package eu.europeana.metis.reprocessing.config;

import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.model.EnrichmentBase;
import eu.europeana.enrichment.api.internal.*;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.DereferencedEntities;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.enrichment.MetisRecordParser;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.rest.client.report.Report;
import eu.europeana.enrichment.utils.EntityMergeEngine;
import eu.europeana.enrichment.utils.RdfEntityUtils;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.web.EntityClientApiImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.utilities.IndexUtilities;
import eu.europeana.metis.reprocessing.utilities.PostProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.*;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.NormalizerStep;
import eu.europeana.normalization.model.NormalizationBatchResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static eu.europeana.enrichment.api.internal.EntityResolver.europeanaLinkPattern;

/**
 * Extra configuration class that is part of {@link Configuration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered internally during the
 * re-processing.</p>
 */
public class DefaultConfiguration extends Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfiguration.class);

    private final ThrowingBiFunction<FullBeanImpl, Configuration, RDF> fullBeanProcessor;
    private final ThrowingTriConsumer<RDF, Boolean, Configuration> rdfIndexer;
    private final ThrowingQuadConsumer<String, Date, Date, Configuration> afterReprocessProcessor;

    private EnrichmentWorker enrichmentWorker;
    private final RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
    private final Normalizer normalizer = new NormalizerFactory().getNormalizer(NormalizerStep.DATES_NORMALIZER);
    private Dereferencer dereferencer;
    private Enricher enricher;
    private EntityMergeEngine entityMergeEngine = new EntityMergeEngine();
    public Map<String, RightsStatmentsValues.RightsValues> aggregationRightsMap = new RightsStatmentsValues().getAggregationRightsMap();

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
        dereferencer = getDereferencer(propertiesHolderExtension);
        enricher = getEnricher(propertiesHolderExtension);
        enrichmentWorker = new EnrichmentWorkerImpl(dereferencer, enricher);
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
        if (StringUtils.isNotBlank(propertiesHolderExtension.dereferenceUrl)) {
            final DereferencerProvider dereferencerProvider = new DereferencerProvider();
            dereferencerProvider.setEnrichmentPropertiesValues(propertiesHolderExtension.entityManagementUrl, propertiesHolderExtension.entityApiUrl, propertiesHolderExtension.entityApiKey);
            dereferencerProvider.setDereferenceUrl(propertiesHolderExtension.dereferenceUrl);
            return dereferencerProvider.create();
        }
        return null;
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
        rdf = translations(rdf);
        rdf = dateNormalization(rdf);
        rdf = europeanaLinksReDereference(rdf);
        rdf = organizationEnrichment(rdf);
        rdf = rightsFix(rdf);

        LOGGER.debug("DONE");
        return rdf;
    }

    RDF translations(RDF rdf) {
        //Get the translation if exists
        FullBeanImpl translationsFullbean = getMongoSourceMongoDao().getTranslationsRecord(rdf.getProvidedCHOList().get(0).getAbout());

        if (translationsFullbean != null) {
            RDF rdfTranslation = EdmUtils.toRDF(translationsFullbean, true);
            Date currentUpdatedDate = rdf.getEuropeanaAggregationList().stream().findFirst().map(EuropeanaAggregationType::getModified).map(Modified::getString)
                    .map(this::convertToDate).orElse(null);
            Date translationUpdateDate = rdfTranslation.getEuropeanaAggregationList().stream().findFirst().map(EuropeanaAggregationType::getModified).map(Modified::getString)
                    .map(this::convertToDate).orElse(null);

            if (currentUpdatedDate != null && translationUpdateDate != null && currentUpdatedDate.compareTo(translationUpdateDate) <= 0
                    && rdf.getProxyList() != null) {
                for (int i = 0; i < rdf.getProxyList().size(); i++) {
                    ProxyType proxyType = rdf.getProxyList().get(i);
                    if (proxyType != null && RdfEntityUtils.isEuropeanaProxy(proxyType)) {
                        rdf.getProxyList().set(i, RdfEntityUtils.getEuropeanaProxy(rdfTranslation));
                        break;
                    }
                }
            }
        }
        return rdf;
    }

    Date convertToDate(String dateString) {
        return Date.from(Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dateString)));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    private RDF dateNormalization(RDF rdf) {
        RDF computedRDF = rdf;
        try {
            LOGGER.debug("Date normalization");
            final String rdfString = rdfConversionUtils.convertRdfToString(rdf);
            final NormalizationBatchResult result = normalizer.normalize(Collections.singletonList(rdfString));
            computedRDF = rdfConversionUtils.convertStringToRdf(result.getNormalizedRecordsInEdmXml().get(0));
        } catch (RuntimeException | SerializationException | NormalizationException e) {
            LOGGER.warn("Something went wrong during enrichment/dereference", e);
        }
        return computedRDF;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    private RDF europeanaLinksReDereference(RDF rdf) {
        LOGGER.debug("Europeana links re-dereference");
        //Find europeana id organizations that are linked in provider aggregation supported fields
        List<Aggregation> aggregationList = rdf.getAggregationList();
        Set<String> aggregationEuropeanaLinks = new HashSet<>();
        for (AggregationFieldType aggregationFieldType : AggregationFieldType.values()) {
            aggregationList.stream().flatMap(aggregationFieldType::extractFields)
                    .map(ResourceOrLiteralType::getResource)
                    .filter(Objects::nonNull)
                    .map(ResourceOrLiteralType.Resource::getResource)
                    .filter(europeanaLinkPattern.asPredicate())
                    .forEach(aggregationEuropeanaLinks::add);
        }

        //Find europeana id non-organization that are linked in europeana proxy supported fields
        final ProxyType europeanaProxy = RdfEntityUtils.getEuropeanaProxy(rdf);
        final Set<String> proxyEuropeanaLinks = Arrays.stream(ProxyFieldType.values())
                .map(proxyFieldType -> proxyFieldType.extractFieldLinksForEnrichment(europeanaProxy))
                .flatMap(Collection::stream)
                .filter(europeanaLinkPattern.asPredicate())
                .collect(Collectors.toSet());

        //Check all present entities and collect links that match
        final Map<Class<? extends AboutType>, Set<String>> entitiesToUpdate = new HashMap<>();
        extendEntitiesMap(entitiesToUpdate, Organization.class, findMatchingEntityLinks(aggregationEuropeanaLinks, rdf::getOrganizationList));
        extendEntitiesMap(entitiesToUpdate, AgentType.class, findMatchingEntityLinks(proxyEuropeanaLinks, rdf::getAgentList));
        extendEntitiesMap(entitiesToUpdate, Concept.class, findMatchingEntityLinks(proxyEuropeanaLinks, rdf::getConceptList));
        extendEntitiesMap(entitiesToUpdate, PlaceType.class, findMatchingEntityLinks(proxyEuropeanaLinks, rdf::getPlaceList));
        extendEntitiesMap(entitiesToUpdate, TimeSpanType.class, findMatchingEntityLinks(proxyEuropeanaLinks, rdf::getTimeSpanList));
        extendEntitiesMap(entitiesToUpdate, Organization.class, findMatchingEntityLinks(proxyEuropeanaLinks, rdf::getOrganizationList));

        //Request new entities
        HashMap<Class<? extends AboutType>, DereferencedEntities> dereferencedEntities = dereferenceEntities(entitiesToUpdate);
        replaceEntities(rdf, dereferencedEntities);

        return rdf;
    }

    private static void replaceEntities(RDF rdf, HashMap<Class<? extends AboutType>, DereferencedEntities> dereferencedEntitiesMap) {
        for (Map.Entry<Class<? extends AboutType>, DereferencedEntities> entry : dereferencedEntitiesMap.entrySet()) {
            DereferencedEntities dereferencedEntities = entry.getValue();
            Map<ReferenceTerm, List<EnrichmentBase>> referenceTermListMap = dereferencedEntities.getReferenceTermListMap();

            List<? extends AboutType> entitiesList = new ArrayList<>();
            if (entry.getKey().isNestmateOf(AgentType.class)) {
                entitiesList = rdf.getAgentList();
            } else if (entry.getKey().isNestmateOf(Concept.class)) {
                entitiesList = rdf.getConceptList();
            } else if (entry.getKey().isNestmateOf(PlaceType.class)) {
                entitiesList = rdf.getPlaceList();
            } else if (entry.getKey().isNestmateOf(TimeSpanType.class)) {
                entitiesList = rdf.getTimeSpanList();
            } else if (entry.getKey().isNestmateOf(Organization.class)) {
                entitiesList = rdf.getOrganizationList();
            }

            findAndReplaceUpdatedEntity(rdf, referenceTermListMap, entitiesList);
        }
    }

    private static void findAndReplaceUpdatedEntity(RDF rdf, Map<ReferenceTerm, List<EnrichmentBase>> referenceTermListMap, List<? extends AboutType> entitiesList) {
        //At this point the dereference values should contain 0 or 1 but not more results per reference
        for (Map.Entry<ReferenceTerm, List<EnrichmentBase>> referenceTermListEntry : referenceTermListMap.entrySet()) {
            if (CollectionUtils.isNotEmpty(referenceTermListEntry.getValue()) && referenceTermListEntry.getValue().get(0) != null) {
                EnrichmentBase enrichmentBase = referenceTermListEntry.getValue().get(0);

                //Find if we have a match and update current rdf
                ListIterator<? extends AboutType> entitiesListIterator = entitiesList.listIterator();
                boolean removedOld = false;
                while (entitiesListIterator.hasNext()) {
                    String about = entitiesListIterator.next().getAbout();
                    if (referenceTermListEntry.getKey().getReference().toString().equals(about)) {
                        entitiesListIterator.remove();
                        removedOld = true;
                        break;
                    }
                }

                //Replace if removed
                if (removedOld) {
                    EntityMergeEngine.convertAndAddEntity(rdf, enrichmentBase);
                }
            }
        }
    }

    private static void extendEntitiesMap(Map<Class<? extends AboutType>, Set<String>> entities, Class<? extends AboutType> classType, Set<String> entitiesToUpdate) {
        entities.computeIfAbsent(classType, v -> new HashSet<>());
        entities.computeIfPresent(classType, (k, v) -> {
            v.addAll(entitiesToUpdate);
            return v;
        });
    }

    private HashMap<Class<? extends AboutType>, DereferencedEntities> dereferenceEntities(Map<Class<? extends AboutType>, Set<String>> entitiesLinksToDereference) {
        HashMap<Class<? extends AboutType>, DereferencedEntities> dereferencedResultEntities = new HashMap<>();

        for (Map.Entry<Class<? extends AboutType>, Set<String>> entry : entitiesLinksToDereference.entrySet()) {
            final HashSet<Report> reports = new HashSet<>();
            Set<ReferenceTerm> referenceTerms = entry.getValue().stream()
                    .map(DefaultConfiguration::getUrl).filter(Objects::nonNull).map(url -> new ReferenceTermImpl(url, new HashSet<>()))
                    .collect(Collectors.toSet());
            final DereferencedEntities dereferencedOwnEntities = dereferencer.dereferenceOwnEntities(referenceTerms, reports, entry.getKey());
            dereferencedResultEntities.put(entry.getKey(), dereferencedOwnEntities);
        }

        return dereferencedResultEntities;
    }

    private static URL getUrl(String link) {
        try {
            return new URL(link);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private static <T extends AboutType> Set<String> findMatchingEntityLinks(Set<String> europeanaLinks, Supplier<List<T>> entitySupplier) {
        return Optional.ofNullable(entitySupplier.get()).orElseGet(Collections::emptyList)
                .stream().map(AboutType::getAbout)
                .filter(europeanaLinks::contains)
                .collect(Collectors.toSet());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    private RDF organizationEnrichment(RDF rdf) {
        LOGGER.debug("Organization re-enrichment");
        final Set<SearchTermContext> searchTerms = new MetisRecordParser().getAggregationSearchTerms(rdf);
        final Pair<Map<SearchTermContext, List<EnrichmentBase>>, Set<Report>> enrichedValues = enricher.enrichValues(searchTerms);

        LOGGER.debug("Merging Enrichment Information...");
        if (enrichedValues.getLeft() != null) {
            for (Map.Entry<SearchTermContext, List<EnrichmentBase>> entry : enrichedValues.getLeft().entrySet()) {
                entityMergeEngine.mergeSearchEntities(rdf, entry.getValue(), entry.getKey());
            }
        }
        return rdf;
    }

//    private RDF generalReEnrichment(RDF rdf) {
//        RDF computedRDF = rdf;
//        try {
//            enrichmentWorker.cleanupPreviousEnrichmentEntities(rdf);
//            ProcessedResult<RDF> rdfProcessedResult = enrichmentWorker.process(rdf, enrichmentWorker.getSupportedModes());
//            computedRDF = rdfProcessedResult.getProcessedRecord() == null ? rdf : rdfProcessedResult.getProcessedRecord();
//        } catch (RuntimeException e) {
//            LOGGER.warn("Something went wrong during enrichment/dereference", e);
//        }
//        return computedRDF;
//    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private RDF rightsFix(RDF rdf) {
        Optional<String> about = rdf.getProvidedCHOList()
                .stream()
                .findFirst()
                .map(ProvidedCHOType::getAbout);

        about.ifPresent(aboutString -> {
            String datasetId = aboutString.substring(1, StringUtils.ordinalIndexOf(aboutString, "/", 2));
            RightsStatmentsValues.RightsValues rightsValues = aggregationRightsMap.get(datasetId);
            if (rightsValues != null) {
                updateAggregationRights(rdf, rightsValues);
                updateWebResourceRights(rdf, datasetId, rightsValues);
                clearWebResourceRights(rdf, datasetId);
            }
        });

        return rdf;
    }

    private void updateAggregationRights(RDF rdf, RightsStatmentsValues.RightsValues rightsValues) {
        rdf.getAggregationList()
                .stream()
                .filter(aggregation -> aggregation.getRights() != null
                        && aggregation.getRights().getResource() != null
                        && rightsValues.getOreAggregation().containsKey(aggregation.getRights().getResource()))
                .forEach(aggregation -> {
                    String newResource = rightsValues.getOreAggregation().get(aggregation.getRights().getResource());
                    aggregation.getRights().setResource(newResource);
                });
    }

    private void updateWebResourceRights(RDF rdf, String bypassDatasetId, RightsStatmentsValues.RightsValues rightsValues) {
        if (rightsValues.isCheckEdmWebResource() && !bypassDatasetId.equals("2048604")) {
            rdf.getWebResourceList()
                    .stream()
                    .filter(webResource -> webResource.getRights() != null
                            && webResource.getRights().getResource() != null
                            && rightsValues.getOreAggregation().containsKey(webResource.getRights().getResource())
                    )
                    .forEach(webResource -> {
                        String newResource = rightsValues.getOreAggregation().get(webResource.getRights().getResource());
                        webResource.getRights().setResource(newResource);
                    });
        }
    }

    private void clearWebResourceRights(RDF rdf, String datasetId) {
        if (datasetId.equals("2048604")) {
            rdf.getWebResourceList().forEach(webResource -> webResource.setRights(null));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
