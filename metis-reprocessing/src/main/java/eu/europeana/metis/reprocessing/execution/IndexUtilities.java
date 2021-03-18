package eu.europeana.metis.reprocessing.execution;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.schema.jibx.RDF;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for indexing.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the indexing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-17
 */
public class IndexUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexUtilities.class);
  private static final Map<Class<?>, String> retryExceptions;

  static {
    retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
    retryExceptions.put(MongoWriteException.class, "E11000 duplicate key error collection");
  }

  private IndexUtilities() {
  }

  /**
   * Indexes a record using properties from a provided {@link BasicConfiguration}
   *
   * @param rdf the source rdf record
   * @param preserveTimestamps should preserve timestamps from source
   * @param basicConfiguration the configuration class that contains required properties
   * @throws IndexingException if an exception occurred during indexing
   */
  public static void indexRecord(RDF rdf, Boolean preserveTimestamps,
      BasicConfiguration basicConfiguration) throws IndexingException {

    try {
      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
      // try-with-resources block
      final IndexerPool indexerPool = basicConfiguration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        indexerPool.indexRdf(rdf, null, preserveTimestamps, null, false);
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }
}
