package eu.europeana.metis.reprocessing.config;

import static eu.europeana.enrichment.api.internal.EntityResolver.europeanaLinkPattern;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.model.EnrichmentBase;
import eu.europeana.enrichment.api.internal.AggregationFieldType;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.api.internal.ReferenceTermContext;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.rest.client.report.Report;
import eu.europeana.enrichment.utils.EntityMergeEngine;
import eu.europeana.entity.client.EntityApiClient;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.utilities.IndexUtilities;
import eu.europeana.metis.reprocessing.utilities.PostProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.Aggregation;
import eu.europeana.metis.schema.jibx.DataProvider;
import eu.europeana.metis.schema.jibx.Organization;
import eu.europeana.metis.schema.jibx.Provider;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.ResourceOrLiteralType;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
  private ClientEntityResolver entityResolver;
  private Enricher enricher;


  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException, EntityClientException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;
    initializeAdditionalElements(propertiesHolderExtension);
  }

  public static String readFileToString(String file) throws IOException {
    ClassLoader classLoader = DefaultConfiguration.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(file);
    if (inputStream == null) {
      throw new IOException("Failed reading file " + file);
    }
    return new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
  }

  public static void renameToMainForTests(String[] args)
      throws IndexingException, DereferenceException, NormalizationConfigurationException,
      TrustStoreConfigurationException, EnrichmentException, URISyntaxException,
      SerializationException, ProcessingException, EntityClientException, IOException {
    DefaultConfiguration defaultConfiguration = new DefaultConfiguration(new PropertiesHolderExtension(
        "application.properties"));

    processAnXmlFile(defaultConfiguration);

    processARecordFromMongoSource(defaultConfiguration);

  }

  private static void processARecordFromMongoSource(DefaultConfiguration defaultConfiguration)
      throws ProcessingException, SerializationException {
    List<FullBeanImpl> fullBeanList = defaultConfiguration.getMongoSourceMongoDao().getRecordsFromList(
        List.of(
            "/954/Culturalia_fd913fb8_8a14_40c9_94ec_38158f4d4c81"
        ));

    for (FullBeanImpl fb : fullBeanList) {

      RDF rdf = defaultConfiguration.getFullBeanProcessor().apply(fb, defaultConfiguration);

      LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
      //      final boolean preserveTimestamps = true;
      //      final Date recordDate = null;
      //      final List<String> datasetIdsForRedirection = null;
      //      final boolean performRedirects = false;
      //      final TierCalculationMode tierCalculationMode = TierCalculationMode.INITIALISE;//defaultConfiguration.getTierCalculationMode();
      //      final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
      //          datasetIdsForRedirection, performRedirects, tierCalculationMode);
      //      IndexerPreprocessor.preprocessRecord(rdf, indexingProperties);
      Set<Report> reports = defaultConfiguration.enricher.enrichment(rdf);
      LOGGER.info("{}\r\n", reports);
      defaultConfiguration.updateOrganizations(rdf);
      LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    }
  }

  private static void processAnXmlFile(DefaultConfiguration defaultConfiguration) throws SerializationException, IOException {
    RDF rdf = defaultConfiguration.rdfConversionUtils.convertStringToRdf(readFileToString("unit_test/test_enrichment.xml"));
    LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    Set<Report> reports = defaultConfiguration.enricher.enrichment(rdf);
    LOGGER.info("{}\r\n", reports);
    defaultConfiguration.updateOrganizations(rdf);
    LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
  }

  private static void extendEntitiesMap(Map<Class<? extends AboutType>, Set<String>> entities,
      Class<? extends AboutType> classType, Set<String> entitiesToUpdate) {
    entities.computeIfAbsent(classType, v -> new HashSet<>());
    entities.computeIfPresent(classType, (k, v) -> {
      v.addAll(entitiesToUpdate);
      return v;
    });
  }

  private static <T extends AboutType> Set<String> findMatchingEntityLinks(Set<String> europeanaLinks,
      Supplier<List<T>> entitySupplier) {
    return Optional.ofNullable(entitySupplier.get()).orElseGet(Collections::emptyList)
                   .stream().map(AboutType::getAbout)
                   .filter(europeanaLinks::contains)
                   .collect(Collectors.toSet());
  }

  private static void replaceEntities(RDF rdf,
      HashMap<Class<? extends AboutType>, Map<ReferenceTermContext, List<EnrichmentBase>>> enrichedEntities) {
    for (Map.Entry<Class<? extends AboutType>, Map<ReferenceTermContext, List<EnrichmentBase>>> entry : enrichedEntities.entrySet()) {
      Map<ReferenceTermContext, List<EnrichmentBase>> dereferencedEntities = entry.getValue();

      List<? extends AboutType> entitiesList = new ArrayList<>();
      if (entry.getKey().isNestmateOf(Organization.class)) {
        entitiesList = rdf.getOrganizationList();
      }

      findAndReplaceUpdatedEntity(rdf, dereferencedEntities, entitiesList);
    }
  }

  private static void findAndReplaceUpdatedEntity(RDF rdf, Map<ReferenceTermContext, List<EnrichmentBase>> referenceTermListMap,
      List<? extends AboutType> entitiesList) {
    //At this point the dereference values should contain 0 or 1 but not more results per reference
    for (Map.Entry<ReferenceTermContext, List<EnrichmentBase>> referenceTermListEntry : referenceTermListMap.entrySet()) {
      if (CollectionUtils.isNotEmpty(referenceTermListEntry.getValue())
          && referenceTermListEntry.getValue().getFirst() != null) {
        EnrichmentBase enrichmentBase = referenceTermListEntry.getValue().getFirst();

        //Find if we have a match and update current rdf
        ListIterator<? extends AboutType> entitiesListIterator = entitiesList.listIterator();
        boolean removedOld = false;
        while (entitiesListIterator.hasNext()) {
          String about = entitiesListIterator.next().getAbout();
          if (referenceTermListEntry.getKey().getReference().toString().equals(about)) {
            entitiesListIterator.remove();
            removedOld = true;
            updateAggregationResources(rdf, about, enrichmentBase);
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

  private static void updateAggregationResources(RDF rdf, String about, EnrichmentBase enrichmentBase) {
    List<Aggregation> list = new ArrayList<>();
    for (Aggregation aggregation : rdf.getAggregationList()) {
      if (aggregation.getDataProvider().getResource() != null
          && aggregation.getDataProvider().getResource().getResource()
                        .equals(about)) {
        DataProvider dataProvider = aggregation.getDataProvider();
        dataProvider.getResource().setResource(enrichmentBase.getAbout());
      }
      if (aggregation.getProvider().getResource() != null
          && aggregation.getProvider().getResource().getResource()
                        .equals(about)) {
        Provider provider = aggregation.getProvider();
        provider.getResource().setResource(enrichmentBase.getAbout());
      }
      if (aggregation.getIntermediateProviderList() != null) {
        aggregation.getIntermediateProviderList().forEach(p -> {
              if (p.getResource().getResource().equals(about)) {
                p.getResource().setResource(enrichmentBase.getAbout());
              }
            }
        );
      }

      list.add(aggregation);
    }
    rdf.setAggregationList(list);
  }

  private static URL getUrl(String link) {
    try {
      return URI.create(link).toURL();
    } catch (MalformedURLException e) {
      return null;
    }
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

    rdf = updateOrganizations(rdf);
    LOGGER.debug("DONE");
    return rdf;
  }

  private void initializeAdditionalElements(PropertiesHolderExtension propertiesHolderExtension)
      throws EntityClientException, EnrichmentException {
    this.entityResolver = (ClientEntityResolver) prepareClientEntityResolver(propertiesHolderExtension);
    this.enricher = getEnricher(propertiesHolderExtension);
  }

  private EntityResolver prepareClientEntityResolver(PropertiesHolderExtension propertiesHolderExtension)
      throws EntityClientException {
    //Sanity check
    if (StringUtils.isAnyBlank(propertiesHolderExtension.entityManagementUrl,
        propertiesHolderExtension.entityApiUrl,
        propertiesHolderExtension.entityApiTokenEndpoint,
        propertiesHolderExtension.entityApiGrantParams)) {
      throw new IllegalArgumentException("Requested resolver but configuration is missing");
    }
    final Properties properties = new Properties();
    properties.put("entity.management.url", propertiesHolderExtension.entityManagementUrl);
    properties.put("entity.api.url", propertiesHolderExtension.entityApiUrl);
    properties.put("token_endpoint", propertiesHolderExtension.entityApiTokenEndpoint);
    properties.put("grant_params", propertiesHolderExtension.entityApiGrantParams);

    return new ClientEntityResolver(new EntityApiClient(new EntityClientConfiguration(properties)));
  }

  private Dereferencer getDereferencer(PropertiesHolderExtension propertiesHolderExtension) throws DereferenceException {
    if (StringUtils.isNotBlank(propertiesHolderExtension.dereferenceUrl)) {
      final DereferencerProvider dereferencerProvider = new DereferencerProvider();
      dereferencerProvider.setEnrichmentPropertiesValues(propertiesHolderExtension.entityManagementUrl,
          propertiesHolderExtension.entityApiUrl, propertiesHolderExtension.entityApiTokenEndpoint,
          propertiesHolderExtension.entityApiGrantParams);
      dereferencerProvider.setDereferenceUrl(propertiesHolderExtension.dereferenceUrl);
      return dereferencerProvider.create();
    }
    return null;
  }

  private Enricher getEnricher(PropertiesHolderExtension propertiesHolderExtension) throws EnrichmentException {
    final EnricherProvider enricherProvider = new EnricherProvider();
    enricherProvider.setEnrichmentPropertiesValues(propertiesHolderExtension.entityManagementUrl,
        propertiesHolderExtension.entityApiUrl, propertiesHolderExtension.entityApiTokenEndpoint,
        propertiesHolderExtension.entityApiGrantParams);
    return enricherProvider.create();
  }

  private HashMap<Class<? extends AboutType>, Map<ReferenceTermContext, List<EnrichmentBase>>> enrichEntities(
      Map<Class<? extends AboutType>, Set<String>> entitiesLinksToDereference) {
    HashMap<Class<? extends AboutType>, Map<ReferenceTermContext, List<EnrichmentBase>>> enrichedResultEntities = new HashMap<>();

    for (Map.Entry<Class<? extends AboutType>, Set<String>> entry : entitiesLinksToDereference.entrySet()) {
      Set<ReferenceTermContext> referenceTerms = entry.getValue().stream()
                                                      .map(DefaultConfiguration::getUrl).filter(Objects::nonNull)
                                                      .map(url -> new ReferenceTermContext(url, new HashSet<>()))
                                                      .collect(Collectors.toSet());
      Map<ReferenceTermContext, List<EnrichmentBase>> enrichedReferences = entityResolver.resolveByUri(referenceTerms);
      enrichedResultEntities.put(entry.getKey(), enrichedReferences);
    }

    return enrichedResultEntities;
  }

  RDF updateOrganizations(RDF rdf) {
    LOGGER.info("Organization Update");
    //Find europeana id organizations that are linked in provider aggregation supported fields
    List<Aggregation> aggregationList = rdf.getAggregationList();
    Set<String> aggregationEuropeanaLinks = new HashSet<>();
    for (AggregationFieldType aggregationFieldType : AggregationFieldType.values()) {
      aggregationList.stream().flatMap(aggregationFieldType::extractFields)
                     .map(ResourceOrLiteralType::getResource)
                     .filter(Objects::nonNull)
                     .map(ResourceOrLiteralType.Resource::getResource)
                     .filter(europeanaLinkPattern.asPredicate()) // check if only this organization
                     .forEach(aggregationEuropeanaLinks::add);
    }
    //Check all present entities and collect links that match
    final Map<Class<? extends AboutType>, Set<String>> entitiesToUpdate = new HashMap<>();
    extendEntitiesMap(entitiesToUpdate, Organization.class,
        findMatchingEntityLinks(aggregationEuropeanaLinks, rdf::getOrganizationList));

    //Request new entities
    HashMap<Class<? extends AboutType>, Map<ReferenceTermContext, List<EnrichmentBase>>> enrichedEntities = enrichEntities(
        entitiesToUpdate);
    replaceEntities(rdf, enrichedEntities);
    return rdf;
  }
}
