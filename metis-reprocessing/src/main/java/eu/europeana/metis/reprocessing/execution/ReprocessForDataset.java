package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
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
  private final String datasetId;
  private final BasicConfiguration basicConfiguration;

  public ReprocessForDataset(String datasetId, BasicConfiguration basicConfiguration) {
    // TODO: 15-5-19 remember to correctly set datasetId
    this.datasetId = "2051942";
    this.basicConfiguration = basicConfiguration;
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
          .info("Processing DatasetId: {} not started because it was already completely processed",
              datasetId);
      return null;
    }
    final long startTime = System.nanoTime();
    loopOverAllRecordsAndProcess(datasetStatus);

    //Code here
//    String dsId = "2051942";
//    final long startTime = System.nanoTime();
//    List<FullBeanImpl> nextPageOfRecords = mongoDao.getNextPageOfRecords(dsId, 0);
//    List<FullBeanImpl> nextPageOfRecords = new ArrayList<>();
//    for (int i = 0; i < 1000; i++) {
//      nextPageOfRecords.add(mongoDao.getRecord(
//          "/03915/public_mistral_memoire_fr_ACTION_CHERCHER_FIELD_1_REF_VALUE_1_AP70L00682F"));
//    }
    final long endTime = System.nanoTime();
    System.out.println(String
        .format("Total time: %s DatasetId: %s ", (double) (endTime - startTime) / 1_000_000_000.0,
            datasetId));

    LOGGER.info("Processing DatasetId: {} end", datasetId);
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
      LOGGER.info("Processing number of records: {}", nextPageOfRecords.size());
      for (FullBeanImpl fullBean : nextPageOfRecords) {
//        processRecord(fullBean);
      }
      datasetStatus.setTotalProcessed(datasetStatus.getTotalProcessed() + nextPageOfRecords.size());
      basicConfiguration.getMongoDestinationMongoDao().storeDatasetStatusToDb(datasetStatus);
      nextPage++;
      nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
          .getNextPageOfRecords(datasetId, nextPage);
    }
    // TODO: 16-5-19 Create all relative information about the reindexing workflow in metis-core
  }

  private void processRecord(FullBeanImpl fullBean, DatasetStatus datasetStatus) {
    try {
      RDF rdf = basicConfiguration.getExtraConfiguration().getFullBeanProcessor()
          .apply(fullBean, basicConfiguration);
      basicConfiguration.getExtraConfiguration().getRdfIndexer()
          .accept(rdf, true, basicConfiguration);
    } catch (IndexingException e) {
      LOGGER.error("Could not index record: {}", fullBean.getAbout(), e);
      if (fullBean.getAbout() != null) {
        datasetStatus.getFailedRecords().add(fullBean.getAbout());
        datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
      }
    } catch (Exception e) {
      LOGGER.error("Could not process record: {}", fullBean.getAbout(), e);
      if (fullBean.getAbout() != null) {
        datasetStatus.getFailedRecords().add(fullBean.getAbout());
        datasetStatus.setTotalFailedRecords(datasetStatus.getTotalFailedRecords() + 1);
      }
    }
  }
}
