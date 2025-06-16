package eu.europeana.metis.removal.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.corelib.definitions.edm.entity.ChangeLog;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.removal.config.Configuration;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.DepublicationReason;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for indexing.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the indexing of records</p>
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
   * Remove tombstone.
   *
   * @param rdf the rdf
   * @param fullBean the full bean
   * @param configuration the configuration
   * @throws IndexingException the indexing exception
   */
  public static void removeTombstone(RDF rdf, FullBeanImpl fullBean, Configuration configuration) throws IndexingException {
    try {
      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
      // try-with-resources block
      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        final String rdfAbout = fullBean.getAbout();

        if (configuration.isDepublicationEnabled()) {
          final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
          // if no detail, erase the whole dataset tombstones
          if (configuration.getRecordIdsToProcess().isEmpty()) {
            tombstoneRemove(datasetId, fullBean, indexerPool);
          } else if (configuration.getRecordIdsToProcess().contains(rdfAbout)) {
            tombstoneRemove(datasetId, fullBean, indexerPool);
          }
        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while tombstone record", e);
    }
  }

  private static void tombstoneRemove(String datasetId, FullBeanImpl fullBean, IndexerPool indexerPool) throws IndexingException {
    boolean isTombStoneRemoved;
    final String rdfAbout = fullBean.getAbout();
    ChangeLog changeLog = fullBean.getEuropeanaAggregation().getChangeLog().getFirst();
    final String BASE_URL = "http://data.europeana.eu/vocabulary/depublicationReason/";

    DepublicationReason reason = getReason(changeLog.getContext().replace(BASE_URL, ""));
    switch (reason) {
      case GDPR, PERMISSION_ISSUES, SENSITIVE_CONTENT -> {
        LOGGER.info("Removing tombstone record {}", rdfAbout);
        LOGGER.info("Tombstone removed record for dataset {} {}", datasetId, rdfAbout);
      }
      case BROKEN_MEDIA_LINKS, REMOVED_DATA_AT_SOURCE, LEGACY, GENERIC -> {
          isTombStoneRemoved = indexerPool.indexTombstone(rdfAbout, reason);
          LOGGER.info("Not applicable reason {}:{}", rdfAbout, reason.getTitle());
          LOGGER.info("Tombstoned {}{}", isTombStoneRemoved, rdfAbout);
      }

      case null, default -> LOGGER.info("Not removing tombstone record {}", rdfAbout);
    }

  }

  private static DepublicationReason getReason(String uriSuffix) {
    switch (uriSuffix) {
      case "contentTier0" -> {
        return DepublicationReason.BROKEN_MEDIA_LINKS;
      }
      case "gdpr" -> {
        return DepublicationReason.GDPR;
      }
      case "noPermission" -> {
        return DepublicationReason.PERMISSION_ISSUES;
      }
      case "sensitiveContent" -> {
        return DepublicationReason.SENSITIVE_CONTENT;
      }
      case "sourceRemoval" -> {
        return DepublicationReason.REMOVED_DATA_AT_SOURCE;
      }
      case "generic" -> {
        return DepublicationReason.GENERIC;
      }
      case "legacy" -> {
        return DepublicationReason.LEGACY;
      }
      case null, default -> {
        return null;
      }
    }
  }

  private static String getDatasetIdOfRecordToBePurged(RDF rdf) {
    Optional<String> about = rdf.getProvidedCHOList()
                                .stream()
                                .filter(Objects::nonNull)
                                .findFirst()
                                .map(ProvidedCHOType::getAbout);

    String result = "";
    if (about.isPresent()) {
      final String[] splitRecordIdentifier = about.get().split("/");
      result = splitRecordIdentifier[1];
    }

    return result;
  }
}
