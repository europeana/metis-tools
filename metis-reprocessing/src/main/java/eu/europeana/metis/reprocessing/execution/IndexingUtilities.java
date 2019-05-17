package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-17
 */
public class IndexingUtilities {

  private IndexingUtilities() {
  }

  public static void indexRdf(RDF rdf, Boolean preserveTimestamps,
      BasicConfiguration basicConfiguration) throws IndexingException {
    final Indexer indexer = basicConfiguration.getIndexer();
    indexer.indexRdf(rdf, preserveTimestamps);
  }
}
