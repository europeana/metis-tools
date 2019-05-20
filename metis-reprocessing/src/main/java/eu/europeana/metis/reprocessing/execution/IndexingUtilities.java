package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.Collections;
import java.util.Map;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-17
 */
public class IndexingUtilities {

  private static final Map<Class<?>, String> exceptionMap = Collections
      .singletonMap(SessionExpiredException.class, "Session expired");

  private IndexingUtilities() {
  }

  public static void indexRdf(RDF rdf, Boolean preserveTimestamps,
      BasicConfiguration basicConfiguration) throws IndexingException {
    try {
      final Indexer indexer = basicConfiguration.getIndexer();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        indexer.indexRdf(rdf, preserveTimestamps);
        return null;
      }, exceptionMap);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }
}
