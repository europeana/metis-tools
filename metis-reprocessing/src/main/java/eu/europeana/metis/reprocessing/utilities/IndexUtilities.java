package eu.europeana.metis.reprocessing.utilities;

import static org.apache.commons.lang3.BooleanUtils.isFalse;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.indexing.tiers.TierCalculationMode;
import eu.europeana.indexing.tiers.model.MediaTier;
import eu.europeana.indexing.utils.RdfTier;
import eu.europeana.indexing.utils.RdfTierUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.jibx.Aggregation;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.EuropeanaType;
import eu.europeana.metis.schema.jibx.EuropeanaType.Choice;
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
import eu.europeana.metis.schema.jibx.ProxyType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.ResourceOrLiteralType;
import eu.europeana.metis.schema.jibx.Type2;
import eu.europeana.metis.utils.DepublicationReason;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

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
        final Set<EdmType> typesEnabledForTierCalculation = EnumSet.of(EdmType._3_D); //<--check this
        final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
            datasetIdsForRedirection, performRedirects, tierCalculationMode, typesEnabledForTierCalculation);
        final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
        if (datasetId.isEmpty()) {
          indexerPool.indexRdf(rdf, indexingProperties);
        } else {
          RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
          List<String> tierData;
          if (RdfTierUtils.hasTierCalculation(rdf, MediaTier.class)) {
            tierData = RdfTierUtils.extractTierData(rdf.getAggregationList(), Aggregation::getHasQualityAnnotationList);
          } else {
            tierData = List.of();
          }
          final String stringRdf = rdfConversionUtils.convertRdfToString(rdf);
          if ((datasetId.equals("9200359")
              && tierData.contains(RdfTier.CONTENT_TIER_1.getUri()))
              || (datasetId.equals("9200579") && hasDcCreator(rdf))
              || (datasetId.equals("2048128") && hasEdmType3D(rdf))
          ) {
            indexerPool.indexTombstone(stringRdf, DepublicationReason.GENERIC);
            indexerPool.remove(stringRdf);
          }
        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while indexing record", e);
    }
  }

  static String getDatasetIdOfRecordToBePurged(RDF rdf) {
    Optional<String> about = rdf.getProvidedCHOList()
                                .stream()
                                .filter(Objects::nonNull)
                                .findFirst()
                                .map(ProvidedCHOType::getAbout);

    String result = "";
    if (about.isPresent()) {
      String datasetId = about.get().substring(1, StringUtils.ordinalIndexOf(about.get(), "/", 2));
      if (datasetId.equals("9200359") || datasetId.equals("9200369") || datasetId.equals("2048128") || datasetId.equals(
          "2048087")) {
        result = datasetId;
      }
    }
    return result;
  }

  static boolean hasDcCreator(RDF rdf) {
    final List<Choice> choices = getProviderProxies(rdf)
        .stream()
        .map(EuropeanaType::getChoiceList)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .toList();
    final List<String> creators = getChoicesInStringList(choices,
        Choice::ifCreator,
        Choice::getCreator,
        ResourceOrLiteralType::getString);

    return creators.contains("Science Museum, London");
  }

  static boolean isProviderProxy(ProxyType proxy) {
    return proxy.getEuropeanaProxy() == null || isFalse(proxy.getEuropeanaProxy().isEuropeanaProxy());
  }

  static List<ProxyType> getProviderProxies(RDF rdf) {
    return Optional.ofNullable(rdf.getProxyList())
                   .stream()
                   .flatMap(Collection::stream)
                   .filter(Objects::nonNull)
                   .filter(IndexUtilities::isProviderProxy)
                   .toList();
  }

  static <T> List<String> getChoicesInStringList(List<Choice> choices, Predicate<Choice> choicePredicate,
      Function<Choice, T> choiceGetter, Function<T, String> getString) {
    return choices.stream()
                  .filter(Objects::nonNull)
                  .filter(choicePredicate)
                  .map(choiceGetter)
                  .map(getString)
                  .toList();
  }

  static boolean hasEdmType3D(RDF rdf) {
    if (rdf.getProxyList() != null && !rdf.getProxyList().isEmpty()) {
      final Set<EdmType> types = rdf.getProxyList()
                                    .stream()
                                    .map(ProxyType::getType)
                                    .filter(Objects::nonNull)
                                    .map(Type2::getType)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toSet());
      if (types.size() == 1) {
        EdmType type = types.iterator().next();
        return type.equals(EdmType._3_D);
      }
    }
    return false;
  }
}
