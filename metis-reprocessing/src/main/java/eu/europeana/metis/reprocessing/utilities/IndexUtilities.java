package eu.europeana.metis.reprocessing.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.indexing.tiers.TierCalculationMode;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.config.DefaultConfiguration;
import eu.europeana.metis.reprocessing.config.PropertiesHolderExtension;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for indexing.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the indexing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019 -05-17
 */
public class IndexUtilities {

  private static final Map<Class<?>, String> retryExceptions;
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
    retryExceptions.put(MongoWriteException.class, "E11000 duplicate key error collection");
  }

  private IndexUtilities() {
  }

  /**
   * Indexes a record using properties from a provided {@link Configuration}
   *
   * @param rdf the source rdf record
   * @param preserveTimestamps should preserve timestamps from source
   * @param configuration the configuration class that contains required properties
   * @throws IndexingException if an exception occurred during indexing
   */
  public static void indexRecord(RDF rdf, Boolean preserveTimestamps, Configuration configuration)
      throws IndexingException {
    try {
      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
      // try-with-resources block
      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        //Timestamps should be preserved, Redirects calculation disabled
        final Date recordDate = null;
        final List<String> datasetIdsForRedirection = null;
        final boolean performRedirects = false;
        final TierCalculationMode tierCalculationMode = configuration.getTierCalculationMode();
        final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
            datasetIdsForRedirection, performRedirects, tierCalculationMode);
        final String rdfAbout = rdf.getProvidedCHOList()
                                   .stream()
                                   .filter(Objects::nonNull)
                                   .map(AboutType::getAbout)
                                   .findFirst()
                                   .orElse("");
        LOGGER.info("Indexing record {}", rdfAbout);
        indexerPool.indexRdf(rdf, indexingProperties);

//        if (configuration.isDepublicationEnabled()) {
//          final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
//          depublishRecord(rdf, datasetId, rdfAbout, indexerPool);
//        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while indexing record", e);
    }
  }

  public static void main(String[] args)
      throws IndexingException, DereferenceException, NormalizationConfigurationException,
      TrustStoreConfigurationException, EntityClientException, EnrichmentException, URISyntaxException, SerializationException, ProcessingException {

    DefaultConfiguration defaultConfiguration = new DefaultConfiguration(new PropertiesHolderExtension(
        "application.properties"));
    RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
    List<FullBeanImpl> fullBeanList = Stream
        .of(
            "/867/https___hispana_mcu_es_lod_oai_prensahistorica_mcu_es_11000468588_ent0",
            "/817/NHMUKXZOOX1935X8X20X138"
        )
        .map(item -> defaultConfiguration.getMongoSourceMongoDao().getRecordsFromList(List.of(item)))
        .flatMap(List::stream)
        .toList();

    for (FullBeanImpl fb : fullBeanList) {
      RDF rdf = defaultConfiguration.getFullBeanProcessor().apply(fb, defaultConfiguration);

      LOGGER.info("Before:\r\n{}\r\n", rdfConversionUtils.convertRdfToString(rdf));
      indexRecord(rdf, true, defaultConfiguration);

      LOGGER.info("After:\r\n{}\r\n", rdfConversionUtils.convertRdfToString(rdf));
    }

  }

//  private static void depublishRecord(RDF rdf, String datasetId, String rdfAbout, IndexerPool indexerPool)
//      throws IndexingException {
//    if ((datasetId.equals("9200359") && hasContentTier(rdf))
//        || (datasetId.equals("9200579") && hasDcCreator(rdf))
//        || (datasetId.equals("2048128") && hasEdmType3D(rdf))
//        || (datasetId.equals("2048087") && hasDataProviders(rdf))
//    ) {
//      boolean isTombStoned;
//      boolean isRemoved;
//      LOGGER.info("Tombstone record for dataset {} {}", datasetId, rdfAbout);
//      isTombStoned = indexerPool.indexTombstone(rdfAbout, DepublicationReason.BROKEN_MEDIA_LINKS);
//      LOGGER.info("Tombstoned record result {} {}", isTombStoned, rdfAbout);
//      LOGGER.info("Remove record for dataset {} {}", datasetId, rdfAbout);
//      isRemoved = indexerPool.removeRecord(rdfAbout);
//      LOGGER.info("Removed record result {} {}", isRemoved, rdfAbout);
//    }
//  }
//
//  private static Pair<String, String> findPrefLabelForOrganization(OrganizationImpl organization) {
//
//    // Try to find an English one first.
//    final List<Pair<String, String>> englishValues = new ArrayList<>(2);
//    Optional.ofNullable(organization.getPrefLabel()).map(labels -> labels.get("en")).stream()
//            .flatMap(List::stream).filter(Objects::nonNull).findFirst()
//            .ifPresent(value -> englishValues.add(new ImmutablePair<>("en", value)));
//    Optional.ofNullable(organization.getPrefLabel()).map(labels -> labels.get("eng")).stream()
//            .flatMap(List::stream).filter(Objects::nonNull).findFirst()
//            .ifPresent(value -> englishValues.add(new ImmutablePair<>("eng", value)));
//    if (!englishValues.isEmpty()) {
//      return englishValues.getFirst();
//    }
//
//    // Otherwise return any value (if available).
//    return Optional.ofNullable(organization.getPrefLabel())
//                   .map(Map::entrySet).stream()
//                   .flatMap(Collection::stream)
//                   .filter(Objects::nonNull).filter(entry -> entry.getValue() != null)
//                   .flatMap(entry -> entry.getValue().stream().filter(StringUtils::isNotBlank)
//                                          .map(value -> new ImmutablePair<>(entry.getKey(), value)))
//                   .findFirst().orElse(null);
//  }
//
//  private static List<AggregationImpl> getDataProviderAggregations(FullBeanImpl fullBean) {
//    List<String> proxyInResult = fullBean.getProxies().stream()
//                                         .filter(not(ProxyImpl::isEuropeanaProxy))
//                                         .filter(proxy -> ArrayUtils.isEmpty(proxy.getLineage())).map(ProxyImpl::getProxyIn)
//                                         .map(Arrays::asList).flatMap(List::stream).toList();
//
//    return fullBean.getAggregations().stream().filter(aggregation -> proxyInResult.contains(aggregation.getAbout())).toList();
//  }
//
//  private static Pair<Set<String>, Map<String, List<String>>> extractUrisAndLiterals(
//      final Map<String, List<String>> urisLiteralsMap, Map<String, Pair<String, String>> organizationPrefLabelMap) {
//    final Set<String> organizationUris = new HashSet<>();
//    final Map<String, List<String>> literalsMap = new HashMap<>();
//
//    if (MapUtils.isNotEmpty(urisLiteralsMap)) {
//      splitOrganizationUrisFromLiterals(urisLiteralsMap, organizationUris, literalsMap, organizationPrefLabelMap);
//
//      //Extend map with organization pref labels
//      if (CollectionUtils.isNotEmpty(organizationUris)) {
//        addOrganizationPrefLabelsToLiterals(organizationUris, literalsMap, organizationPrefLabelMap);
//      }
//    }
//    return new ImmutablePair<>(organizationUris, literalsMap);
//  }
//
//  private static void splitOrganizationUrisFromLiterals(Map<String, List<String>> urisLiteralsMap,
//      Set<String> organizationUris, Map<String, List<String>> literalsMap,
//      Map<String, Pair<String, String>> organizationPrefLabelMap) {
//    for (Map.Entry<String, List<String>> entry : urisLiteralsMap.entrySet()) {
//      final List<String> literals = new ArrayList<>();
//      for (String value : entry.getValue()) {
//        if (organizationPrefLabelMap.containsKey(value)) {
//          organizationUris.add(value);
//        } else {
//          literals.add(value);
//        }
//      }
//      if (!literals.isEmpty()) {
//        literalsMap.put(entry.getKey(), literals);
//      }
//    }
//  }
//
//  private static void addOrganizationPrefLabelsToLiterals(Set<String> organizationUris,
//      Map<String, List<String>> literalsMap, Map<String, Pair<String, String>> organizationPrefLabelMap) {
//    for (String organizationUri : organizationUris) {
//      final Pair<String, String> entry = organizationPrefLabelMap.get(organizationUri);
//      if (entry != null) {
//        literalsMap.computeIfAbsent(entry.getKey(), key -> new ArrayList<>()).add(entry.getValue());
//      }
//    }
//  }
//
//  private static String getDatasetIdOfRecordToBePurged(RDF rdf) {
//    Optional<String> about = rdf.getProvidedCHOList()
//                                .stream()
//                                .filter(Objects::nonNull)
//                                .findFirst()
//                                .map(ProvidedCHOType::getAbout);
//
//    String result = "";
//    if (about.isPresent()) {
//      final String[] splitRecordIdentifier = about.get().split("/");
//      String datasetId = splitRecordIdentifier[1];
//      if (datasetId.equals("9200359") || datasetId.equals("9200579")
//          || datasetId.equals("2048128") || datasetId.equals("2048087")) {
//        result = datasetId;
//      }
//    }
//    return result;
//  }
//
//  private static boolean isProviderProxy(ProxyType proxy) {
//    return proxy.getEuropeanaProxy() == null || isFalse(proxy.getEuropeanaProxy().isEuropeanaProxy());
//  }
//
//  private static List<ProxyType> getProviderProxies(RDF rdf) {
//    return Optional.ofNullable(rdf.getProxyList())
//                   .stream()
//                   .flatMap(Collection::stream)
//                   .filter(Objects::nonNull)
//                   .filter(IndexUtilities::isProviderProxy)
//                   .toList();
//  }
//
//  private static <T> List<String> getChoicesInStringList(List<Choice> choices, Predicate<Choice> choicePredicate,
//      Function<Choice, T> choiceGetter, Function<T, String> getString) {
//    return choices.stream()
//                  .filter(Objects::nonNull)
//                  .filter(choicePredicate)
//                  .map(choiceGetter)
//                  .map(getString)
//                  .toList();
//  }
//
//  /**
//   * MET-6360 Has dc creator boolean.
//   *
//   * @param rdf the rdf
//   * @return the boolean
//   */
//  static boolean hasDcCreator(RDF rdf) {
//    final List<Choice> choices = getProviderProxies(rdf)
//        .stream()
//        .map(EuropeanaType::getChoiceList)
//        .filter(Objects::nonNull)
//        .flatMap(Collection::stream)
//        .toList();
//    final List<String> creators = getChoicesInStringList(choices,
//        Choice::ifCreator,
//        Choice::getCreator,
//        ResourceOrLiteralType::getString);
//    boolean result = creators.contains("Science Museum, London");
//    if (result) {
//      LOGGER.info("Has DC creator: {}", creators);
//      return true;
//    } else {
//      return false;
//    }
//  }
//
//  /**
//   * MET-6361 Has edm type 3D boolean.
//   *
//   * @param rdf the rdf
//   * @return the boolean
//   */
//  static boolean hasEdmType3D(RDF rdf) {
//    if (rdf.getProxyList() != null && !rdf.getProxyList().isEmpty()) {
//      final Set<EdmType> types = rdf.getProxyList()
//                                    .stream()
//                                    .map(ProxyType::getType)
//                                    .filter(Objects::nonNull)
//                                    .map(Type2::getType)
//                                    .filter(Objects::nonNull)
//                                    .collect(Collectors.toSet());
//      if (types.size() == 1) {
//        EdmType type = types.iterator().next();
//        boolean result = type.equals(EdmType._3_D);
//        if (result) {
//          LOGGER.info("Has EdmType3d: {}", type);
//          return true;
//        } else {
//          return false;
//        }
//      }
//    }
//    return false;
//  }
//
//  /**
//   * MET-6362 Has data providers boolean.
//   *
//   * @param rdf the rdf
//   * @return the boolean
//   */
//  static boolean hasDataProviders(RDF rdf) {
//    RdfToFullBeanConverter rdfToFullBeanConverter = new RdfToFullBeanConverter();
//    FullBeanImpl fullBean = rdfToFullBeanConverter.convertRdfToFullBean(new RdfWrapper(rdf));
//
//    final Map<String, Pair<String, String>> organizationPrefLabelMap =
//        fullBean.getOrganizations()
//                .stream()
//                .filter(org -> StringUtils.isNotBlank(org.getAbout()))
//                .collect(
//                    Collectors.toMap(OrganizationImpl::getAbout,
//                        IndexUtilities::findPrefLabelForOrganization,
//                        (o1, o2) -> o1));
//
//    AggregationImpl aggregation = getDataProviderAggregations(fullBean).getFirst();
//
//    final Pair<Set<String>, Map<String, List<String>>> dataProviderPair =
//        extractUrisAndLiterals(aggregation.getEdmDataProvider(), organizationPrefLabelMap);
//
//    if (dataProviderPair.getValue().isEmpty()) {
//      return false;
//    } else {
//      if (dataProviderPair.getKey().contains(rdf.getAggregationList().getFirst().getDataProvider().getResource().getResource())) {
//        String dataProvider = "";
//
//        if (dataProviderPair.getValue().values().stream().findFirst().isPresent()) {
//          dataProvider = dataProviderPair.getValue().values().stream().findFirst().get().getFirst();
//        }
//
//        if (IndexUtilities.DATA_PROVIDERS.contains(dataProvider)) {
//          LOGGER.info("Has DataProvider: {} => {}", fullBean.getAbout(), dataProvider);
//          return true;
//        }
//      }
//    }
//    return false;
//  }
}
