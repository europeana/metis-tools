package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessForDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessForDataset.class);
  private final String prefixDatasetidLog;
  private final String datasetId;
  private final BasicConfiguration basicConfiguration;

  public ReprocessForDataset(String datasetId, BasicConfiguration basicConfiguration) {
    this.datasetId = datasetId;
    this.basicConfiguration = basicConfiguration;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
  }

  @Override
  public Void call() {
    return reprocessDataset();
  }

  private Void reprocessDataset() {
    LOGGER.info("Processing DatasetId: {} started", datasetId);
    final DatasetStatus datasetStatus = retrieveOrInitializeDatasetStatus();
    if (datasetStatus.getTotalRecords() == datasetStatus.getTotalProcessed()) {
      LOGGER
          .info("{} - Processing not started because it was already completely processed",
              prefixDatasetidLog);
      return null;
    }
    loopOverAllRecordsAndProcess(datasetStatus);
    LOGGER.info("{} - Processing end", prefixDatasetidLog);
    LOGGER.info("{} - DatasetStatus - {}", prefixDatasetidLog, datasetStatus);
    LOGGER.info(STATISTICS_LOGS_MARKER, "{} - DatasetStatus - {}", prefixDatasetidLog,
        datasetStatus);
    return null;
  }

  private DatasetStatus retrieveOrInitializeDatasetStatus() {
    DatasetStatus datasetStatus = basicConfiguration.getMongoDestinationMongoDao()
        .getDatasetStatus(datasetId);
    if (datasetStatus == null) {
      datasetStatus = new DatasetStatus();
      final long totalRecordsForDataset = basicConfiguration.getMongoSourceMongoDao()
          .getTotalRecordsForDataset(datasetId);
      datasetStatus.setDatasetId(datasetId);
      datasetStatus.setTotalRecords(totalRecordsForDataset);
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
    }
    return datasetStatus;
  }

  private int getNextPage(DatasetStatus datasetStatus) {
    final long totalProcessed = datasetStatus.getTotalProcessed();
    return (int) (totalProcessed / MongoSourceMongoDao.PAGE_SIZE);
  }

  private void loopOverAllRecordsAndProcess(final DatasetStatus datasetStatus) {
    int nextPage = getNextPage(datasetStatus);
    List<FullBeanImpl> nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
        .getNextPageOfRecords(datasetId, nextPage);
    while (CollectionUtils.isNotEmpty(nextPageOfRecords)) {
      LOGGER.info("{} - Processing number of records: {}", prefixDatasetidLog,
          nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
        processAndIndex(datasetStatus, fullBean);
      }
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      nextPage++;
      nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
          .getNextPageOfRecords(datasetId, nextPage);
    }
    // TODO: 16-5-19 Create all relative information about the reindexing workflow in metis-core
  }

  private void processAndIndex(DatasetStatus datasetStatus, FullBeanImpl fullBean) {
    try {
      final RDF rdf = processRecord(fullBean, datasetStatus);
      indexRecord(rdf, datasetStatus);
    } catch (ProcessingException | IndexingException e) {
      String stepString;
      if (e instanceof ProcessingException) {
        stepString = "process";
      } else {
        stepString = "index";
      }
      LOGGER.error("{} - Could not {} record: {}", prefixDatasetidLog, stepString,
          fullBean.getAbout(), e);
      if (fullBean.getAbout() != null) {
        datasetStatus.getFailedRecordsSet().add(fullBean.getAbout());
        datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
      }
    } finally {
      datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + 1);
    }
  }

  private RDF processRecord(FullBeanImpl fullBean, DatasetStatus datasetStatus)
      throws ProcessingException {
    final long startTimeProcess = System.nanoTime();
    try {
      return basicConfiguration.getExtraConfiguration().getFullBeanProcessor()
          .apply(fullBean, basicConfiguration);
    } finally {
      final long endTimeProcess = System.nanoTime();
      final long newAverage = addValueToAverage(datasetStatus.getTotalProcessed() + 1,
          datasetStatus.getAverageTimeRecordProcessing(), endTimeProcess - startTimeProcess);
      datasetStatus.setAverageTimeRecordProcessing(newAverage);
    }
  }

  private void indexRecord(RDF rdf, DatasetStatus datasetStatus)
      throws IndexingException {
    final long startTimeIndex = System.nanoTime();
    try {
      basicConfiguration.getExtraConfiguration().getRdfIndexer()
          .accept(rdf, true, basicConfiguration);
    } finally {
      final long endTimeIndex = System.nanoTime();
      final long newAverage = addValueToAverage(datasetStatus.getTotalProcessed() + 1,
          datasetStatus.getAverageTimeRecordIndexing(), endTimeIndex - startTimeIndex);
      datasetStatus.setAverageTimeRecordIndexing(newAverage);
    }
  }

  private long addValueToAverage(long totalSamples, long oldAverage, long newValue) {
    return oldAverage + ((newValue - oldAverage) / totalSamples);
  }
}
