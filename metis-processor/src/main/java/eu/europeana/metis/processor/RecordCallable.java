package eu.europeana.metis.processor;

import com.mongodb.MongoWriteException;
import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.utilities.RdfUtil;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.RDF;
import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordCallable implements Callable<RDF> {

  private static final Map<Class<?>, String> retryExceptions;
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
    retryExceptions.put(MongoWriteException.class, "E11000 duplicate key error collection");
  }

  private final FullBeanImpl fullBean;
  private final IndexerPool indexerPool;

  public RecordCallable(FullBeanImpl fullBean, IndexerPool indexerPool) {
    this.fullBean = fullBean;
    this.indexerPool = indexerPool;
  }

  @Override
  public RDF call() throws Exception {
    final long startTime = System.nanoTime();
    final RDF rdf = EdmUtils.toRDF(fullBean, true);
    if (RdfUtil.hasThumbnailsAndValidLicense(rdf)) {
      LOGGER.info("Thread: {} - Processing RDF: {}", Thread.currentThread().getName(),
          rdf.getProvidedCHOList().getFirst().getAbout());
          indexRdf(rdf);
    } else {
      LOGGER.debug("Thread: {} - Skipping RDF: {}", Thread.currentThread().getName(),
          rdf.getProvidedCHOList().getFirst().getAbout());
    }
    final long elapsedTimeInNanoSec = System.nanoTime() - startTime;
    LOGGER.info("Elapsed time for whole processing {}ns", elapsedTimeInNanoSec);
    return rdf;
  }

  private void indexRdf(RDF rdf) throws RecordRelatedIndexingException {
    //Timestamps should be preserved, Redirects calculation disabled
    final Date recordDate = null;
    final List<String> datasetIdsForRedirection = null;
    final boolean performRedirects = false;
    final boolean tierRecalculation = false;
    final boolean preserveTimestamps = true;
    final Set<EdmType> typesEnabledForTierCalculation = EnumSet.of(EdmType._3_D);
    final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
        datasetIdsForRedirection, performRedirects, tierRecalculation, typesEnabledForTierCalculation);

    try {
      ExternalRequestUtil.retryableExternalRequest(() -> {
        indexerPool.indexRdf(rdf, indexingProperties);
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
    }
  }

}
