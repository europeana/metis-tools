package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Callable} class, processing records page by page for a specific dataset.
 * <p>Obtaining page numbers is performed by an internal asynchronous thread that calls a
 * synchronous operation of {@link ProcessDataset#getNextPageAndIncrement()}. Each page of
 * {@link FullBeanImpl} records is requested asynchronously and stored in an internal limited {@link
 * BlockingQueue} (FullBeanImpl)}. The main thread processes available items from the queue and for
 * each list of records calls {@link ProcessDataset#processRecords(List)}</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-11
 */
public class PageProcess implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PageProcess.class);
  private final String prefixDatasetidLog;
  private final ProcessDataset processDataset;

  public PageProcess(ProcessDataset processDataset, String datasetId) {
    this.processDataset = processDataset;
    this.prefixDatasetidLog = String.format("DatasetId: %s", datasetId);
  }

  @Override
  public Integer call() {
    int nextPage = processDataset.getNextPageAndIncrement();
    List<FullBeanImpl> nextPageOfRecords = processDataset.getFullBeans(nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Processing page: {}, range of records: {} - {}",
          prefixDatasetidLog, nextPage, nextPage * MongoSourceMongoDao.PAGE_SIZE,
          ((nextPage + 1) * MongoSourceMongoDao.PAGE_SIZE) - 1);
      processDataset.processRecords(nextPageOfRecords);
      processDataset.updateDatasetStatus(nextPage);
      nextPage = processDataset.getNextPageAndIncrement();
      nextPageOfRecords = processDataset.getFullBeans(nextPage);
    }
    return nextPageOfRecords == null ? 0 : nextPageOfRecords.size();
  }

}
