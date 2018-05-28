package eu.europeana.metis.endpoints.mapper.utilities;

import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dao.WorkflowDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.rest.ResponseListWrapper;
import eu.europeana.metis.core.workflow.Workflow;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.IndexToPreviewPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.IndexToPublishPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.OaipmhHarvestPluginMetadata;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles execution of the modes
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String WORKFLOW_WITH_DATASET_ID_UPDATED_IN_ORIGINAL_DATABASE_TEMPLATE = "Workflow with datasetId: {}, updated in original database";
  private static final String NEXT_PAGE_WITH_WORKFLOW_LIST_SIZE_TEMPLATE = "NextPage: {}, with workflow list size: {}";
  private static final String WORKFLOW_PROCESSED_COUNTER_TEMPLATE = "WorkflowProcessedCounter: {}";

  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDaoOriginal;
  private final WorkflowDao workflowDaoOriginal;
  private final WorkflowDao workflowDaoTemporary;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDaoOriginal,
      WorkflowDao workflowDaoOriginal, WorkflowDao workflowDaoTemporary) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDaoOriginal = datasetDaoOriginal;
    this.workflowDaoOriginal = workflowDaoOriginal;
    this.workflowDaoTemporary = workflowDaoTemporary;
  }

  public void copyWorkflowsMode() {

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start the copying of workflows");
    int workflowProcessedCounter = 0;
    int nextPage = 0;
    do {
      //Read pages from original database
      ResponseListWrapper<Workflow> responseListWrapper = new ResponseListWrapper<>();
      responseListWrapper.setResultsAndLastPage(
          workflowDaoOriginal.getAllWorkflows(propertiesHolder.organizationId, nextPage),
          workflowDaoOriginal.getWorkflowsPerRequest(), nextPage);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          NEXT_PAGE_WITH_WORKFLOW_LIST_SIZE_TEMPLATE, nextPage, responseListWrapper.getListSize());
      nextPage = responseListWrapper.getNextPage();

      //Copy original workflow to temporary database
      responseListWrapper.getResults().forEach(workflow -> {
        workflowDaoTemporary.create(workflow);
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Workflow with datasetId: {}, created in temporary database", workflow.getDatasetId());
      });
      workflowProcessedCounter += responseListWrapper.getListSize();
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          WORKFLOW_PROCESSED_COUNTER_TEMPLATE, workflowProcessedCounter);
    } while (nextPage != -1);
  }

  public void createWorkflows(Mode mode) {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start the creation of the map");
    int workflowProcessedCounter = 0;
    int nextPage = 0;
    do {
      //Read pages from original database
      ResponseListWrapper<Workflow> responseListWrapper = new ResponseListWrapper<>();
      responseListWrapper.setResultsAndLastPage(
          workflowDaoOriginal.getAllWorkflows(propertiesHolder.organizationId, nextPage),
          workflowDaoOriginal.getWorkflowsPerRequest(), nextPage);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          NEXT_PAGE_WITH_WORKFLOW_LIST_SIZE_TEMPLATE, nextPage, responseListWrapper.getListSize());
      nextPage = responseListWrapper.getNextPage();

      //Overwrite workflow with europeana oai endpoint in original database
      responseListWrapper.getResults()
          .forEach(workflow -> overwriteWorkflowInDatabase(workflow, mode));
      workflowProcessedCounter += responseListWrapper.getListSize();
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          WORKFLOW_PROCESSED_COUNTER_TEMPLATE, workflowProcessedCounter);
    } while (nextPage != -1);
  }

  private void overwriteWorkflowInDatabase(Workflow workflow, Mode mode) {
    List<AbstractMetisPluginMetadata> metisPluginsMetadata = new ArrayList<>(1);
    if (mode == Mode.CREATE_OAIPMH_WORKFLOWS) {
      Dataset dataset = datasetDaoOriginal.getDatasetByDatasetId(workflow.getDatasetId());
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(propertiesHolder.europeanaOaiEndpoint);
      oaipmhHarvestPluginMetadata.setMetadataFormat("edm");
      oaipmhHarvestPluginMetadata.setSetSpec(dataset.getDatasetName());
      metisPluginsMetadata.add(oaipmhHarvestPluginMetadata);
      workflow.setMetisPluginsMetadata(metisPluginsMetadata);
    } else if (mode == Mode.CREATE_PREVIEW_WORKFLOWS) {
      metisPluginsMetadata.add(new IndexToPreviewPluginMetadata());
      workflow.setMetisPluginsMetadata(metisPluginsMetadata);
    } else if (mode == Mode.CREATE_PUBLISH_WORKFLOWS) {
      metisPluginsMetadata.add(new IndexToPublishPluginMetadata());
      workflow.setMetisPluginsMetadata(metisPluginsMetadata);
    }
    metisPluginsMetadata.get(0).setMocked(false);
    metisPluginsMetadata.get(0).setEnabled(true);
    workflowDaoOriginal.update(workflow);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        WORKFLOW_WITH_DATASET_ID_UPDATED_IN_ORIGINAL_DATABASE_TEMPLATE, workflow.getDatasetId());
  }

  public void reverseWorkflows() {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Start the reversing to the original information of the map");
    int workflowProcessedCounter = 0;
    int nextPage = 0;
    do {
      //Read pages from temporary database
      ResponseListWrapper<Workflow> responseListWrapper = new ResponseListWrapper<>();
      responseListWrapper.setResultsAndLastPage(
          workflowDaoTemporary.getAllWorkflows(propertiesHolder.organizationId, nextPage),
          workflowDaoTemporary.getWorkflowsPerRequest(), nextPage);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          NEXT_PAGE_WITH_WORKFLOW_LIST_SIZE_TEMPLATE, nextPage, responseListWrapper.getListSize());
      nextPage = responseListWrapper.getNextPage();

      //Replace workflows(backed up) from the temporary to the original database
      responseListWrapper.getResults().forEach(workflow -> {
        workflowDaoOriginal.update(workflow);
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            WORKFLOW_WITH_DATASET_ID_UPDATED_IN_ORIGINAL_DATABASE_TEMPLATE,
            workflow.getDatasetId());
      });
      workflowProcessedCounter += responseListWrapper.getListSize();
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          WORKFLOW_PROCESSED_COUNTER_TEMPLATE, workflowProcessedCounter);
    } while (nextPage != -1);
  }

}
