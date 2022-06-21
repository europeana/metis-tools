package eu.europeana.metis.reprocessing.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.RDF;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        final boolean tierRecalculation = configuration.isTierRecalculation();
        final Set<EdmType> typesEnabledForTierCalculation = EnumSet.of(EdmType._3_D);
        final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
            datasetIdsForRedirection, performRedirects, tierRecalculation, typesEnabledForTierCalculation);
        indexerPool.indexRdf(rdf, indexingProperties);
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }
}
