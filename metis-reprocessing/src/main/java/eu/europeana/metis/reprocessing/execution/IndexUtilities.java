package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.schema.jibx.RDF;
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
      ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
        try {
          indexerPool.indexRdf(rdf, null, preserveTimestamps, null, false);
        } catch (IndexingException e) {
          LOGGER.warn(EXECUTION_LOGS_MARKER, "Could not index rdf with about {}",
              rdf.getProvidedCHOList().get(0).getAbout());
        }
        return null;
      });
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }
}
