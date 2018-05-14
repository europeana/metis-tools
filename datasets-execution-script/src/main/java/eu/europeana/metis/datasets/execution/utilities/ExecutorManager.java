package eu.europeana.metis.datasets.execution.utilities;

import eu.europeana.metis.RestEndpoints;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.rest.ResponseListWrapper;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * Handles execution of the modes
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String NEXT_PAGE_WITH_DATASET_LIST_SIZE_TEMPLATE = "NextPage: {}, with dataset list size: {}";
  private static final long MONITOR_INTERVAL_IN_SECONDS = 5;
  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDao;
  private final String startDatasetExecutionUrl;
  private final String getWorkflowExecutionUrl;
  private final RestTemplate restTemplate = new RestTemplate();

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDao) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDao = datasetDao;
    startDatasetExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_DATASETID_EXECUTE;
    getWorkflowExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_EXECUTIONS_EXECUTIONID;
    restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
  }

  public void startExecutions() throws InterruptedException {
    int nextPage = 0;
    do {
      //Read pages from database
      ResponseListWrapper<Dataset> responseListWrapper = new ResponseListWrapper<>();
      responseListWrapper.setResultsAndLastPage(
          datasetDao.getAllDatasetsByOrganizationId(propertiesHolder.organizationId, nextPage),
          datasetDao.getDatasetsPerRequest(), nextPage);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          NEXT_PAGE_WITH_DATASET_LIST_SIZE_TEMPLATE, nextPage, responseListWrapper.getListSize());
      nextPage = responseListWrapper.getNextPage();

      for (Dataset dataset : responseListWrapper.getResults()) {
        handleDatasetExecution(dataset);
      }
    } while (nextPage != -1);
  }

  private void handleDatasetExecution(Dataset dataset) throws InterruptedException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting datasetId {} execution.",
        dataset.getDatasetId());
//      WorkflowExecution workflowExecution = sendDatasetForExecution(dataset.getDatasetId());
    WorkflowExecution workflowExecution = sendDatasetForExecution("15");
    do {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Requesting WorkflowExecution for datasetId {} execution.", dataset.getDatasetId());
      workflowExecution = monitorWorkflowExecution(workflowExecution.getId().toString());
      AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "WorkflowExecution status info: datasetId {}, EcloudDatasetId: {}, ExecutionId: {}, PluginType: {}, ExternalTaskId: {}, PluginStatus: {}, ExpectedRecords: {}, ProcessedRecords: {}, ErrorRecords: {}, TaskStatus: {}",
          dataset.getDatasetId(), dataset.getEcloudDatasetId(),
          workflowExecution.getId().toString(),
          abstractMetisPlugin.getPluginType(), abstractMetisPlugin.getExternalTaskId(),
          abstractMetisPlugin.getPluginStatus(),
          abstractMetisPlugin.getExecutionProgress().getExpectedRecords(),
          abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
          abstractMetisPlugin.getExecutionProgress().getErrors(),
          abstractMetisPlugin.getExecutionProgress().getStatus());
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(MONITOR_INTERVAL_IN_SECONDS));
      } catch (InterruptedException e) {
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Exception occurred during sleep time",
            e);
        throw e;
      }
    } while (workflowExecution.getWorkflowStatus() != WorkflowStatus.FINISHED
        && workflowExecution.getWorkflowStatus() != WorkflowStatus.FAILED
        && workflowExecution.getWorkflowStatus() != WorkflowStatus.CANCELLED);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Ended datasetId {} executionId {} and final status {}",
        dataset.getDatasetId(), workflowExecution.getId().toString(),
        workflowExecution.getWorkflowStatus());
    AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
    LOGGER.info(PropertiesHolder.FINAL_DATASET_STATUS,
        "datasetId {}, EcloudDatasetId: {}, ExecutionId: {}, PluginType: {}, ExternalTaskId: {}, PluginStatus: {}, ExpectedRecords: {}, ProcessedRecords: {}, ErrorRecords: {}, TaskStatus: {}",
        dataset.getDatasetId(), dataset.getEcloudDatasetId(), workflowExecution.getId().toString(),
        abstractMetisPlugin.getPluginType(), abstractMetisPlugin.getExternalTaskId(),
        abstractMetisPlugin.getPluginStatus(),
        abstractMetisPlugin.getExecutionProgress().getExpectedRecords(),
        abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
        abstractMetisPlugin.getExecutionProgress().getErrors(),
        abstractMetisPlugin.getExecutionProgress()
            .getStatus());//Log only the status of the end result
  }

  private WorkflowExecution sendDatasetForExecution(String datasetId) {
    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("datasetId", datasetId);

    return restTemplate
        .postForObject(startDatasetExecutionUrl, null, WorkflowExecution.class, pathVariables);
  }

  private WorkflowExecution monitorWorkflowExecution(String executionId) {
    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("executionId", executionId);

    return restTemplate
        .getForObject(getWorkflowExecutionUrl, WorkflowExecution.class, pathVariables);
  }
}
