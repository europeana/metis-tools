package eu.europeana.metis.depublishing.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.depublishing.config.Configuration;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
import eu.europeana.metis.schema.jibx.RDF;
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
   * Indexes a record using properties from a provided {@link Configuration}
   *
   * @param rdf the source rdf record
   * @param configuration the configuration class that contains required properties
   * @throws IndexingException if an exception occurred during indexing
   */
  public static void removeTombstone(RDF rdf, Configuration configuration)
      throws IndexingException {
    try {
      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
      // try-with-resources block
      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        final String rdfAbout = rdf.getProvidedCHOList()
                                   .stream()
                                   .filter(Objects::nonNull)
                                   .map(AboutType::getAbout)
                                   .findFirst()
                                   .orElse("");
        LOGGER.info("Removing tombstone record {}", rdfAbout);

        if (configuration.isDepublicationEnabled()) {
          final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
          // if no detail, erase the whole dataset tombstones
          if (configuration.getRecordIdsToProcess().isEmpty()) {
            tombstoneRemove(datasetId, rdfAbout, indexerPool);
          } else if (configuration.getRecordIdsToProcess().contains(rdfAbout)) {
            tombstoneRemove(datasetId, rdfAbout, indexerPool);
          }
        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while tombstone record", e);
    }
  }

  private static void tombstoneRemove(String datasetId, String rdfAbout, IndexerPool indexerPool) throws IndexingException {
    boolean isRecordRemoved;
    boolean isTombStoneRemoved;
    LOGGER.info("Tombstone removed record for dataset {} {}", datasetId, rdfAbout);
    isTombStoneRemoved = indexerPool.removeTombstone(rdfAbout);
    LOGGER.info("Tombstone removed record result {} {}", isTombStoneRemoved, rdfAbout);
    LOGGER.info("Remove record for dataset {} {}", datasetId, rdfAbout);
    isRecordRemoved = indexerPool.removeRecord(rdfAbout);
    LOGGER.info("Remove record result {} {}", isRecordRemoved, rdfAbout);
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
