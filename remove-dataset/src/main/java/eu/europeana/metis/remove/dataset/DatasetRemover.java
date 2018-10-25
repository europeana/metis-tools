package eu.europeana.metis.remove.dataset;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dao.DatasetXsltDao;
import eu.europeana.metis.core.dao.ScheduledWorkflowDao;
import eu.europeana.metis.core.dao.WorkflowDao;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import java.util.Iterator;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetRemover {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetRemover.class);

  private final DataSetServiceClient datasetServiceClient;
  private final RecordServiceClient recordServiceClient;
  private final String providerId;

  private final DatasetDao datasetDao;
  private final WorkflowDao workflowDao;
  private final WorkflowExecutionDao workflowExecutionDao;
  private final ScheduledWorkflowDao scheduledWorkflowDao;
  private final DatasetXsltDao datasetXsltDao;

  public DatasetRemover(MorphiaDatastoreProvider morphiaDatastoreProvider,
      DataSetServiceClient datasetServiceClient, RecordServiceClient recordServiceClient,
      String providerId) {

    this.datasetServiceClient = datasetServiceClient;
    this.recordServiceClient = recordServiceClient;
    this.providerId = providerId;

    this.datasetDao = new DatasetDao(morphiaDatastoreProvider, null);
    this.datasetXsltDao = new DatasetXsltDao(morphiaDatastoreProvider);
    this.workflowDao = new WorkflowDao(morphiaDatastoreProvider);
    this.workflowExecutionDao = new WorkflowExecutionDao(morphiaDatastoreProvider);
    this.scheduledWorkflowDao = new ScheduledWorkflowDao(morphiaDatastoreProvider);
  }

  public void removeDataset(String metisDatasetId) throws MCSException {

    // Get the dataset: if it doesn't exist, we are done.
    final Dataset dataset = datasetDao.getDatasetByDatasetId(metisDatasetId);
    if (dataset == null) {
      LOGGER.info("  * Cannot remove dataset {}: dataset does not exist.", metisDatasetId);
      return;
    }

    // JV: Check that no processing is currently happening on this dataset (or cancel all tasks).

    // Get eCloud dataset ID
    final String ecloudDatasetId = dataset.getEcloudDatasetId();
    LOGGER.info("  * Starting removal of dataset {} ({}) with eCloud ID {}.", metisDatasetId,
        dataset.getDatasetName(), ecloudDatasetId);

    // JV: Check if the ecloud ID exists (has been created): otherwise, skip next steps.

    // Remove all representations of this dataset in eCloud
    LOGGER.info("  * Removing all representations in eCloud.");
    final Iterator<Representation> representations = datasetServiceClient
        .getRepresentationIterator(providerId, ecloudDatasetId);
    final RecordDeleter recordDeleter = new RecordDeleter();
    final Thread logThread = new Thread(() -> regularCountLog(recordDeleter));
    logThread.start();
    try {
      representations.forEachRemaining(recordDeleter);
    } finally {
      logThread.interrupt();
    }
    LOGGER.info("  * Finished removing representations: {} representations removed from eCloud.",
        recordDeleter.count);

    // Remove dataset in eCloud
    LOGGER.info("  * Removing dataset in eCloud.");
    datasetServiceClient.deleteDataSet(providerId, ecloudDatasetId);

    // Removing all workflow data from Metis.
    LOGGER.info("  * Removing all workflow data from Metis.");
    workflowExecutionDao.deleteAllByDatasetId(metisDatasetId);
    scheduledWorkflowDao.deleteAllByDatasetId(metisDatasetId);
    workflowDao.deleteWorkflow(metisDatasetId);
    datasetXsltDao.deleteAllByDatasetId(metisDatasetId);
    datasetDao.delete(dataset);
  }

  private static void regularCountLog(RecordDeleter recordDeleter) {
    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        return;
      }
      if (Thread.interrupted()) {
        return;
      } else {
        LOGGER.info("        {} representations deleted.", recordDeleter.count);
      }
    }
  }

  private class RecordDeleter implements Consumer<Representation> {

    int count = 0;

    @Override
    public void accept(Representation representation) {
      try {
        recordServiceClient.deleteRepresentation(representation.getCloudId(),
            representation.getRepresentationName(), representation.getVersion());
      } catch (MCSException e) {
        throw new RuntimeException(e);
      }
      count++;
    }
  }
}
