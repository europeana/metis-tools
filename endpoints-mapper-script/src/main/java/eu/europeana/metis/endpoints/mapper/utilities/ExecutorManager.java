package eu.europeana.metis.endpoints.mapper.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.Workflow;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.IndexToPreviewPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.IndexToPublishPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.OaipmhHarvestPluginMetadata;

/**
 * Handles execution of the modes
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String WORKFLOW_WITH_DATASET_ID_UPDATED_IN_ORIGINAL_DATABASE_TEMPLATE =
      "Workflow with datasetId: {}, updated in original database";
  private static final String WORKFLOW_LIST_SIZE_TEMPLATE = "Workflow list size: {}";
  private static final String WORKFLOW_PROCESSED_COUNTER_TEMPLATE = "WorkflowProcessedCounter: {}";

  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDaoOriginal;
  private final ExtendedWorkflowDao workflowDaoOriginal;
  private final ExtendedWorkflowDao workflowDaoTemporary;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDaoOriginal,
      ExtendedWorkflowDao workflowDaoOriginal, ExtendedWorkflowDao workflowDaoTemporary) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDaoOriginal = datasetDaoOriginal;
    this.workflowDaoOriginal = workflowDaoOriginal;
    this.workflowDaoTemporary = workflowDaoTemporary;
  }

  private static List<Workflow> getWorkflowsForOrganizationId(String organizationId,
      DatasetDao datasetSource, ExtendedWorkflowDao workflowSource) {
    final Set<String> datasetIds = datasetSource.getAllDatasetsByOrganizationId(organizationId)
        .stream().map(Dataset::getDatasetId).collect(Collectors.toSet());
    return workflowSource.getAllWorkflows(datasetIds);
  }

  public void copyWorkflowsMode() {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start the copying of workflows");

    // Obtain the workflows
    final List<Workflow> workflows = getWorkflowsForOrganizationId(propertiesHolder.organizationId,
        datasetDaoOriginal, workflowDaoOriginal);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_LIST_SIZE_TEMPLATE,
        workflows.size());

    // Copy original workflows to temporary database
    workflows.forEach(workflow -> {
      workflowDaoTemporary.create(workflow);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Workflow with datasetId: {}, created in temporary database", workflow.getDatasetId());
    });
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_PROCESSED_COUNTER_TEMPLATE,
        workflows.size());
  }

  public void createWorkflows(Mode mode) {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start the creation of the map");

    // Obtain the workflows
    final List<Workflow> workflows = getWorkflowsForOrganizationId(propertiesHolder.organizationId,
        datasetDaoOriginal, workflowDaoOriginal);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_LIST_SIZE_TEMPLATE,
        workflows.size());

    // Overwrite workflow with europeana oai endpoint in original database
    workflows.forEach(workflow -> overwriteWorkflowInDatabase(workflow, mode));
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_PROCESSED_COUNTER_TEMPLATE,
        workflows.size());
  }

  private void overwriteWorkflowInDatabase(Workflow workflow, Mode mode) {
    List<AbstractMetisPluginMetadata> metisPluginsMetadata = new ArrayList<>(1);
    if (mode == Mode.CREATE_OAIPMH_WORKFLOWS) {
      Dataset dataset = datasetDaoOriginal.getDatasetByDatasetId(workflow.getDatasetId());
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(propertiesHolder.europeanaOaiEndpoint);
      oaipmhHarvestPluginMetadata.setMetadataFormat("edm");
      oaipmhHarvestPluginMetadata.setSetSpec(dataset.getDatasetId());
      oaipmhHarvestPluginMetadata.setDatasetId(workflow.getDatasetId());
      oaipmhHarvestPluginMetadata.setUseDefaultIdentifiers(true);
      oaipmhHarvestPluginMetadata.setIdentifierPrefixRemoval("http://data.europeana.eu/item");
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

    // Obtain the workflows
    final List<Workflow> workflows = getWorkflowsForOrganizationId(propertiesHolder.organizationId,
        datasetDaoOriginal, workflowDaoTemporary);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_LIST_SIZE_TEMPLATE,
        workflows.size());

    // Replace workflows(backed up) from the temporary to the original database
    workflows.forEach(workflow -> {
      workflowDaoOriginal.update(workflow);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          WORKFLOW_WITH_DATASET_ID_UPDATED_IN_ORIGINAL_DATABASE_TEMPLATE, workflow.getDatasetId());
    });
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, WORKFLOW_PROCESSED_COUNTER_TEMPLATE,
        workflows.size());
  }
}
