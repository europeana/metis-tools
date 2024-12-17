package eu.europeana.metis.processor;

import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.processor.utilities.RdfUtil;
import eu.europeana.metis.schema.jibx.RDF;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordCallable implements Callable<RDF> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final FullBeanImpl fullBean;

  public RecordCallable(FullBeanImpl fullBean) {
    this.fullBean = fullBean;
  }

  @Override
  public RDF call() throws Exception {
    // TODO: 25/07/2023 Can we implement it with steps?

    final long startTime = System.nanoTime();

    final RDF rdf = EdmUtils.toRDF(fullBean, true);
    if (RdfUtil.hasThumbnailsAndValidLicense(rdf)) {
      LOGGER.info("Thread: {} - Processing RDF: {}", Thread.currentThread().getName(),
          rdf.getProvidedCHOList().get(0).getAbout());

    } else {
      LOGGER.debug("Thread: {} - Skipping RDF: {}", Thread.currentThread().getName(), rdf.getProvidedCHOList().get(0).getAbout());
    }
    final long elapsedTimeInNanoSec = System.nanoTime() - startTime;
    LOGGER.info("Elapsed time for whole processing {}ns", elapsedTimeInNanoSec);
    return rdf;
  }
}
