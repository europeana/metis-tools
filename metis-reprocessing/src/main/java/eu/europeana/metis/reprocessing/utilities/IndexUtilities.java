package eu.europeana.metis.reprocessing.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.indexing.tiers.TierCalculationMode;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.RDF;
//import eu.europeana.metis.utils.DepublicationReason;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains functionality for indexing.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the indexing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-17
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
        indexerPool.indexRdf(rdf, indexingProperties);
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }

//  public static void indexTombstone(String rdfAbout, DepublicationReason depublicationReason, Configuration configuration)
//      throws IndexingException {
//    try {
//      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
//      // try-with-resources block
//      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
//      ExternalRequestUtil.retryableExternalRequest(() -> {
//        indexerPool.indexTombstone(rdfAbout, depublicationReason);
//        return null;
//      }, retryExceptions);
//    } catch (Exception e) {
//      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
//    }
//  }

}
