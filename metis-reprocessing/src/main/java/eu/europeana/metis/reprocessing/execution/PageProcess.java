package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-11
 */
public class PageProcess implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PageProcess.class);
  private final String prefixDatasetidLog;
  private final ReprocessForDataset reprocessForDataset;
  private final String datasetId;

  public PageProcess(ReprocessForDataset reprocessForDataset, String datasetId) {
    this.reprocessForDataset = reprocessForDataset;
    this.datasetId = datasetId;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
  }

  @Override
  public Integer call() {
    int nextPage = reprocessForDataset.getNextPageAndIncrement();
    List<FullBeanImpl> nextPageOfRecords = reprocessForDataset.getFullBeans(nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info(EXECUTION_LOGS_MARKER, "{} - Processing page: {}, range of records: {} - {}",
          prefixDatasetidLog, nextPage, nextPage * MongoSourceMongoDao.PAGE_SIZE,
          ((nextPage + 1) * MongoSourceMongoDao.PAGE_SIZE) - 1);
      reprocessForDataset.processRecords(nextPageOfRecords);
      reprocessForDataset.updateDatasetStatus(nextPage);
      nextPage = reprocessForDataset.getNextPageAndIncrement();
      nextPageOfRecords = reprocessForDataset.getFullBeans(nextPage);
    }
    return nextPageOfRecords == null ? 0 : nextPageOfRecords.size();
  }

}
