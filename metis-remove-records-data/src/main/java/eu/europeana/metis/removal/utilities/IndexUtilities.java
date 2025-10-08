package eu.europeana.metis.removal.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.removal.config.Configuration;
import eu.europeana.metis.removal.config.Mode;
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
   * Remove record.
   *
   * @param rdf the rdf
   * @param fullBean the full bean
   * @param configuration the configuration
   * @throws IndexingException the indexing exception
   */
  public static void removeRecord(RDF rdf, FullBeanImpl fullBean, Configuration configuration) throws IndexingException {
    try {
      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        final String rdfAbout = fullBean.getAbout();
        final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
        if (configuration.getRecordIdsToProcess().contains(rdfAbout)) {
          if (configuration.getMode().equals(Mode.DRY_RUN)) {
            LOGGER.info("{} removing records from dataset {} {}", datasetId, rdfAbout, configuration.getMode().name());
          } else {
            LOGGER.info("Removing records from dataset {} {}", datasetId, rdfAbout);
            indexerPool.removeRecord(rdfAbout);
          }
        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while removing record", e);
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
