package eu.europeana.metis.reprocessing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver.OperationMode;
import eu.europeana.enrichment.api.external.model.EnrichmentBase;
import eu.europeana.enrichment.api.internal.AggregationFieldType;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.api.internal.FieldType;
import eu.europeana.enrichment.api.internal.FieldValue;
import eu.europeana.enrichment.api.internal.ReferenceTermContext;
import eu.europeana.enrichment.api.internal.SearchTermContext;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
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
import eu.europeana.metis.schema.jibx.Organization;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    List<FullBeanImpl> fullBeanList = Stream
        .of(
            //i
            "/2020702/raa_fmi_10000100970001",
            //f
            "/2048087/ProvidedCHO_Battersea_Arts_Centre_BAC_9_YT_002_006_002",
            //e
            "/2048128/114145",
            //d
            "/9200579/kyaq8pq9",
            //c
            "/9200359/BibliographicResource_3000123626519",
            "/9200359/BibliographicResource_3000100585617", //with contentTier 4
            "/9200359/BibliographicResource_3000100387622", //with contentTier 1
            //a
            "/9200579/cynwkevu",
            //b
            "/954/Culturalia_fd913fb8_8a14_40c9_94ec_38158f4d4c81",
            //g
            "/1087/https___catalonica_bnc_cat_catalonicahub_lod_oai_arca_bnc_cat_10000296883_ent0",
            //h
            "/164/https___catalonica_bnc_cat_catalonicahub_lod_oai_ddd_uab_cat_100377_ent1"
        )
        .map(item -> defaultConfiguration.getMongoSourceMongoDao().getRecordsFromList(List.of(item)))
        .flatMap(List::stream)
        .toList();

    for (FullBeanImpl fb : fullBeanList) {
      RDF rdf = defaultConfiguration.getFullBeanProcessor().apply(fb, defaultConfiguration);

      LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
      // tier calculation
      // ------------------------------
      //      final boolean preserveTimestamps = true;
      //      final Date recordDate = null;
      //      final List<String> datasetIdsForRedirection = null;
      //      final boolean performRedirects = false;
      //      final TierCalculationMode tierCalculationMode = TierCalculationMode.INITIALISE;//defaultConfiguration.getTierCalculationMode();
      //      final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
      //          datasetIdsForRedirection, performRedirects, tierCalculationMode);
      //      IndexerPreprocessor.preprocessRecord(rdf, indexingProperties);
      defaultConfiguration.updateOrganizations(rdf);
      LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    }

  }

  private static void processAnXmlFile(DefaultConfiguration defaultConfiguration) throws SerializationException, IOException {
    RDF rdf = defaultConfiguration.rdfConversionUtils.convertStringToRdf(
        readFileToString("unit_test/test_enrichment_organizations_2.xml"));
    LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    defaultConfiguration.updateOrganizations(rdf);
    LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
  }

  private static String convertToValidURLString(String link) {
    try {
      return URI.create(link).toURL().toString();
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

  private void initializeAdditionalElements(PropertiesHolderExtension propertiesHolderExtension) throws EntityClientException {
    this.entityResolver = (ClientEntityResolver) prepareClientEntityResolver(propertiesHolderExtension);
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

    return new ClientEntityResolver(new EntityApiClient(new EntityClientConfiguration(properties)), OperationMode.CACHED);
  }

  /**
   * Updates europeana id organizations that are linked in provider aggregation supported fields.
   * <p>
   * 1. Text/literal, for enrichment by text and updates with a reference(link). 2. Resource, see all organizations present in the
   * record. If it finds a resource reference(link) is present in an aggregation and is not europeana does an enriching by uri and
   * replaces the reference(link). 3. Resource, and if an europeana is present, does the organization update.
   *
   * @param rdf record
   * @return rdf with updated organizations.
   */
  RDF updateOrganizations(RDF rdf) {
    LOGGER.info("Organization Update");
    //Find europeana id organizations that are linked in provider aggregation supported fields
    List<Aggregation> aggregations = Optional.ofNullable(rdf.getAggregationList())
                                             .stream()
                                             .flatMap(Collection::stream)
                                             .filter(Objects::nonNull)
                                             .toList();

    final Map<FieldValue, Set<FieldType<Aggregation>>> aggregationLiteralValuesMap = new HashMap<>();
    final Map<String, Set<FieldType<?>>> aggregationReferencesMap = new HashMap<>();

    for (AggregationFieldType aggregationFieldType : AggregationFieldType.values()) {
      // case no. 1 collect all values
      aggregations.stream()
                  .map(aggregationFieldType::extractFieldValuesForEnrichment)
                  .flatMap(Collection::stream)
                  .forEach(fieldValue -> aggregationLiteralValuesMap
                      .computeIfAbsent(fieldValue, value -> new HashSet<>())
                      .add(aggregationFieldType)
                  );

      // case no. 2 and case no. 3 collect all valid url references
      aggregations.stream()
                  .map(aggregationFieldType::extractFieldLinksForEnrichment)
                  .flatMap(Collection::stream)
                  .map(DefaultConfiguration::convertToValidURLString)
                  .filter(Objects::nonNull)
                  .forEach(referenceLink -> aggregationReferencesMap
                      .computeIfAbsent(referenceLink, value -> new HashSet<>())
                      .add(aggregationFieldType)
                  );
    }

    // case no.1 enrich values and add a reference(link)
    // ***************************************************************************************************************************
    final Set<SearchTermContext> searchTermsContext = aggregationLiteralValuesMap
        .entrySet()
        .stream()
        .map(literalValuesMap ->
            new SearchTermContext(literalValuesMap.getKey().value(),
                literalValuesMap.getKey().language(), literalValuesMap.getValue()))
        .collect(Collectors.toSet());

    Map<SearchTermContext, List<EnrichmentBase>> enrichedValues = entityResolver.resolveByText(searchTermsContext);
    EntityMergeEngine entityMergeEngine = new EntityMergeEngine();

    // update with organization id's case no. 1
    enrichedValues.forEach(
        (searchTermContext, enrichmentBases) -> entityMergeEngine.mergeEntities(rdf, enrichmentBases, searchTermContext));

    // case no. 2 and case no. 3
    // ***************************************************************************************************************************

    final Set<ReferenceTermContext> allReferenceTerms = aggregationReferencesMap
        .entrySet()
        .stream()
        .map(referencesMap -> ReferenceTermContext
            .createFromString(referencesMap.getKey(), referencesMap.getValue()))
        .collect(Collectors.toSet());

    final Set<String> existingOrganisationIds = Optional
        .ofNullable(rdf.getOrganizationList())
        .stream()
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .map(AboutType::getAbout)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());

    final Set<String> existingOtherEntityIds = Stream
        .of(rdf.getAgentList(), rdf.getConceptList(), rdf.getPlaceList(), rdf.getTimeSpanList())
        .filter(Objects::nonNull).flatMap(Collection::stream)
        .filter(Objects::nonNull).map(AboutType::getAbout)
        .filter(Objects::nonNull).collect(Collectors.toSet());

    final Set<ReferenceTermContext> referencesToResolve = allReferenceTerms
        .stream()
        .filter(referenceTermContext ->
            !existingOrganisationIds.contains(referenceTermContext.getReferenceAsString()))
        .filter(referenceTermContext ->
            !existingOtherEntityIds.contains(referenceTermContext.getReferenceAsString()))
        .collect(Collectors.toSet());

    final Set<ReferenceTermContext> referencesToUpdate = allReferenceTerms
        .stream()
        .filter(referenceTermContext ->
            existingOrganisationIds.contains(referenceTermContext.getReferenceAsString()))
        .collect(Collectors.toSet());

    final Map<ReferenceTermContext, List<EnrichmentBase>> enrichedReferencesResolved = entityResolver.resolveByUri(
        referencesToResolve);
    // update with organizations id's case no. 2
    enrichedReferencesResolved.forEach(
        (referenceTermContext, enrichmentBases) -> entityMergeEngine.mergeEntities(rdf, enrichmentBases, referenceTermContext));

    final Map<ReferenceTermContext, List<EnrichmentBase>> enrichedReferencesToUpdate = entityResolver.resolveByUri(
        referencesToUpdate);

    final Map<String, Organization> organisationMap = Optional
        .ofNullable(rdf.getOrganizationList())
        .stream()
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .filter(organization -> organization.getAbout() != null)
        .collect(Collectors.toMap(AboutType::getAbout, Function.identity()));
    // remove old organizations id's
    enrichedReferencesToUpdate.forEach(
        (referenceTermContext, enrichmentBases) -> organisationMap.remove(referenceTermContext.getReferenceAsString()));
    rdf.setOrganizationList(new ArrayList<>(organisationMap.values()));
    // update with new organization id's case no. 3
    enrichedReferencesToUpdate.forEach(
        (referenceTermContext, enrichmentBases) -> entityMergeEngine.mergeEntities(rdf, enrichmentBases, referenceTermContext));

    LOGGER.info("reference cache:{}",entityResolver.cacheReferenceTermStats());
    LOGGER.info("search cache:{}",entityResolver.cacheSearchTermStats());
    LOGGER.info("entity cache:{}",entityResolver.cacheEntityStats());
    return rdf;
  }
}
