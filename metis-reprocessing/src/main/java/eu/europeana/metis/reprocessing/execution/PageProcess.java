package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Callable} class, processing records page by page for a specific dataset.
 * <p>Obtaining page numbers is performed by an internal asynchronous thread that calls a
 * synchronous operation of {@link ReprocessForDataset#getNextPageAndIncrement()}. Each page of
 * {@link FullBeanImpl} records is requested asynchronously and stored in an internal limited {@link
 * BlockingQueue} (FullBeanImpl)}. The main thread processes available items from the queue and for each
 * list of records calls {@link ReprocessForDataset#processRecords(List)}</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-11
 */
public class PageProcess implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PageProcess.class);
  private final String prefixDatasetidLog;
  private final ReprocessForDataset reprocessForDataset;
  private BlockingQueue<Map.Entry<Integer, List<FullBeanImpl>>> entriesToProcessQueue = new ArrayBlockingQueue<>(
      5);
  private volatile boolean morePagesAvailable = true;

  PageProcess(ReprocessForDataset reprocessForDataset, String datasetId) {
    this.reprocessForDataset = reprocessForDataset;
    this.prefixDatasetidLog = String.format("DatasetId: %s", datasetId);
    new Thread(new AsynchronousFullbeanRetriever()).start();
  }

  @Override
  public Integer call() throws InterruptedException {
    Integer nextPage;
    List<FullBeanImpl> nextPageOfRecords = null;
    Entry<Integer, List<FullBeanImpl>> nextEntry;
    do {
      nextEntry = entriesToProcessQueue.poll(5, TimeUnit.SECONDS);
      if (nextEntry != null) {
        nextPage = nextEntry.getKey();
        nextPageOfRecords = nextEntry.getValue();
        LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Processing page: {}, range of records: {} - {}",
            prefixDatasetidLog, nextPage, nextPage * MongoSourceMongoDao.PAGE_SIZE,
            ((nextPage + 1) * MongoSourceMongoDao.PAGE_SIZE) - 1);
        reprocessForDataset.processRecords(nextPageOfRecords);
        reprocessForDataset.updateDatasetStatus(nextPage);
      }
    } while (morePagesAvailable);
    return nextPageOfRecords == null ? 0 : nextPageOfRecords.size();
  }

  private class AsynchronousFullbeanRetriever implements Runnable {

    @Override
    public void run() {
      int nextPage = reprocessForDataset.getNextPageAndIncrement();
      List<FullBeanImpl> nextPageOfRecords = reprocessForDataset.getFullBeans(nextPage);
      try {
        while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
          entriesToProcessQueue.put(new AbstractMap.SimpleEntry<>(nextPage, nextPageOfRecords));
          nextPage = reprocessForDataset.getNextPageAndIncrement();
          nextPageOfRecords = reprocessForDataset.getFullBeans(nextPage);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Fullbean queue population interrupted.", e);
        Thread.currentThread().interrupt();
      }

      //Check until all pages are obtained
      try {
        while (!entriesToProcessQueue.isEmpty()) {
          Thread.sleep(5000);
        }
        morePagesAvailable = false;
      } catch (InterruptedException e) {
        LOGGER.error("Fullbean queue waiting for cleanup interrupted.", e);
        Thread.currentThread().interrupt();
      }
    }
  }

}
