package eu.europeana.metis.reprocessing.execution;

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
public class IndexingUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingUtilities.class);

  private IndexingUtilities() {
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
      final IndexerPool indexerPool = basicConfiguration.getIndexerPool();
      ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
        try {
          indexerPool.indexRdf(rdf, null, preserveTimestamps, null, false);
        } catch (IndexingException e) {
          LOGGER.warn("Could not index rdf with about {}",
              rdf.getProvidedCHOList().get(0).getAbout());
        }
        return null;
      });
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }
}
