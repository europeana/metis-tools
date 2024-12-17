package eu.europeana.metis.processor;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.schema.jibx.RDF;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ExecutorService threadPool;

  private final ExecutorCompletionService<RDF> completionService;

  public RecordsProcessor(int maxThreads) {
    this.threadPool = Executors.newFixedThreadPool(maxThreads);
    this.completionService = new ExecutorCompletionService<>(threadPool);
  }

  public List<RDF> process(List<FullBeanImpl> fullBeans) throws Exception {

    List<Future<RDF>> futureList = new ArrayList<>(fullBeans.size());
    for (FullBeanImpl fullbean : fullBeans) {
      RecordCallable recordCallable = new RecordCallable(fullbean);
      futureList.add(completionService.submit(recordCallable));
    }

    List<RDF> rdfs = new ArrayList<>(fullBeans.size());
    try {
      for (int i = 0; i < fullBeans.size(); i++) {
        Future<RDF> future = completionService.take();
        futureList.remove(future);
        rdfs.add(future.get());
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Exception processing the future", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    } finally {
      for (Future<RDF> future : futureList) {
        future.cancel(true);
      }
      for (int j = 0; j < futureList.size(); j++) {
        completionService.take();
      }
    }

    return rdfs;
  }

  public void close() {
    threadPool.shutdown();
    LOGGER.info("Thread pool closed.");
  }
}
