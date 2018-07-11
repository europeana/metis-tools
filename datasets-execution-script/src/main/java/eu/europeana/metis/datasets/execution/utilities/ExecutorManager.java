package eu.europeana.metis.datasets.execution.utilities;

import eu.europeana.metis.RestEndpoints;
import eu.europeana.metis.authentication.user.MetisUser;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.rest.ResponseListWrapper;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Handles execution of the modes
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String NEXT_PAGE_WITH_DATASET_LIST_SIZE_TEMPLATE = "NextPage: {}, with dataset list size: {}";
  private static final String PATH_TO_PROCESSED_DATASETS_FILE = "./datasets-execution/logs/processed-datasets.log";
  public static final String AUTHORIZATION = "Authorization";
  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDao;
  private final String loginUserUrl;
  private final String startDatasetExecutionUrl;
  private final String getWorkflowExecutionUrl;
  private final RestTemplate restTemplate = new RestTemplate();
  private String accessToken;
  private List<String> processedDatasetIds = new ArrayList<>();

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDao) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDao = datasetDao;
    loginUserUrl = this.propertiesHolder.metisAuthenticationHost
        + RestEndpoints.AUTHENTICATION_LOGIN;
    startDatasetExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_DATASETID_EXECUTE;
    getWorkflowExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_EXECUTIONS_EXECUTIONID;
    restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
  }

  public void startExecutions() throws InterruptedException, IOException {
    initializeProcessedDatasetList(); //Read file that contains dataset ids that are already processed from previous executions
    int nextPage = 0;
    int processedDatasetsCounter = 0;
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
        if (!processedDatasetIds.contains(dataset.getDatasetId())) {
          handleDatasetExecution(dataset);
          processedDatasetsCounter++;
        }
      }
    } while (nextPage != -1
        && processedDatasetsCounter < propertiesHolder.numberOfDatasetsToProcess);
  }

  private void initializeProcessedDatasetList() throws IOException {
    try (Stream<String> stream = Files
        .lines(Paths.get(PATH_TO_PROCESSED_DATASETS_FILE), StandardCharsets.UTF_8)) {
      stream.forEach(line -> processedDatasetIds.add(line));
      if (!processedDatasetIds.isEmpty()) {
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Using Processed datasets file: {}. There are {} datasets that will be bypassed.",
            PATH_TO_PROCESSED_DATASETS_FILE, processedDatasetIds.size());
      }
    }
  }

  private void handleDatasetExecution(Dataset dataset) throws InterruptedException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting datasetId {} execution.",
        dataset.getDatasetId());
    WorkflowExecution workflowExecution = sendDatasetForExecution(dataset.getDatasetId());
    do {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Requesting WorkflowExecution for datasetId: {} execution.", dataset.getDatasetId());
      workflowExecution = monitorWorkflowExecution(workflowExecution.getId().toString());
      AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "WorkflowExecution status info: datasetId: {}, EcloudDatasetId: {}, ExecutionId: {}, PluginType: {}, ExternalTaskId: {}, PluginStatus: {}, ExpectedRecords: {}, ProcessedRecords: {}, ErrorRecords: {}, TaskStatus: {}",
          dataset.getDatasetId(), dataset.getEcloudDatasetId(),
          workflowExecution.getId(),
          abstractMetisPlugin.getPluginType(), abstractMetisPlugin.getExternalTaskId(),
          abstractMetisPlugin.getPluginStatus(),
          abstractMetisPlugin.getExecutionProgress().getExpectedRecords(),
          abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
          abstractMetisPlugin.getExecutionProgress().getErrors(),
          abstractMetisPlugin.getExecutionProgress().getStatus());
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(propertiesHolder.monitorIntervalInSecs));
      } catch (InterruptedException e) {
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Exception occurred during sleep time",
            e);
        throw e;
      }
    } while (workflowExecution.getWorkflowStatus() != WorkflowStatus.FINISHED
        && workflowExecution.getWorkflowStatus() != WorkflowStatus.FAILED
        && workflowExecution.getWorkflowStatus() != WorkflowStatus.CANCELLED);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Ended datasetId: {} executionId: {} and final status: {}",
        dataset.getDatasetId(), workflowExecution.getId(),
        workflowExecution.getWorkflowStatus());
    AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
    LOGGER.info(PropertiesHolder.FINAL_DATASET_STATUS,
        "datasetId: {}, EcloudDatasetId: {}, ExecutionId: {}, PluginType: {}, ExternalTaskId: {}, PluginStatus: {}, ExpectedRecords: {}, ProcessedRecords: {}, ErrorRecords: {}, TaskStatus: {}",
        dataset.getDatasetId(), dataset.getEcloudDatasetId(), workflowExecution.getId(),
        abstractMetisPlugin.getPluginType(), abstractMetisPlugin.getExternalTaskId(),
        abstractMetisPlugin.getPluginStatus(),
        abstractMetisPlugin.getExecutionProgress().getExpectedRecords(),
        abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
        abstractMetisPlugin.getExecutionProgress().getErrors(),
        abstractMetisPlugin.getExecutionProgress()
            .getStatus());//Log only the status of the end result
    LOGGER.info(PropertiesHolder.PROCESSED_DATASETS,
        dataset.getDatasetId()); //Gets appended and is not timestamp based
  }

  private WorkflowExecution sendDatasetForExecution(String datasetId) {

    accessToken = loginAndGetAuthorizationToken();
    HttpHeaders accessTokenHeader = new HttpHeaders();
    accessTokenHeader
        .set(AUTHORIZATION, "Bearer " + accessToken);

    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("datasetId", datasetId);

    // Query parameters
    UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(startDatasetExecutionUrl)
        .queryParam("enforcedPluginType", propertiesHolder.enforcedPluginType);

    return restTemplate
        .postForObject(builder.buildAndExpand(pathVariables).toUri().toString(),
            new HttpEntity<>(null, accessTokenHeader),
            WorkflowExecution.class, pathVariables);
  }

  private WorkflowExecution monitorWorkflowExecution(String executionId) {
    HttpHeaders accessTokenHeader = new HttpHeaders();
    accessTokenHeader
        .set(AUTHORIZATION, "Bearer " + accessToken);
    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("executionId", executionId);

    return restTemplate.exchange
        (getWorkflowExecutionUrl, HttpMethod.GET, new HttpEntity<>(null, accessTokenHeader),
            WorkflowExecution.class, pathVariables).getBody();
  }

  private String loginAndGetAuthorizationToken() {
    UriComponentsBuilder authenticationBuilder = UriComponentsBuilder.fromUriString(loginUserUrl);
    HttpHeaders authenticationHttpHeaders = new HttpHeaders();
    authenticationHttpHeaders.set(AUTHORIZATION, "Basic " + new String(Base64.getEncoder()
        .encode((propertiesHolder.metisUsername + ":" + propertiesHolder.metisPassword).getBytes()),
        Charset.forName("UTF-8")));

    MetisUser metisUser = restTemplate
        .postForObject(authenticationBuilder.build().toUri().toString(),
            new HttpEntity<>(null, authenticationHttpHeaders), MetisUser.class);
    return metisUser.getMetisUserAccessToken().getAccessToken();
  }
}
