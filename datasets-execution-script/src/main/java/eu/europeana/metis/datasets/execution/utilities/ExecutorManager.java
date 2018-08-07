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
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
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
  private static final String PREFIX_OF_PROCESSED_DATASET_FILE = "./datasets-execution/logs/processed-datasets-";
  private static final String LOG_FILE_EXTENSION = ".log";
  public static final int DURATION_OF_NO_RECORD_CHANGE_IN_MINS = 30;
  private final Marker processedDatasetsMarker;
  private final String pathToProcessedDatasetsFile;
  public static final String AUTHORIZATION = "Authorization";
  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDao;
  private final String loginUserUrl;
  private final String startDatasetExecutionUrl;
  private final String getWorkflowExecutionUrl;
  private final String cancelWorkflowExecutionUrl;
  private final RestTemplate restTemplate = new RestTemplate();
  private String accessToken;
  private List<String> processedDatasetIds = new ArrayList<>();
  private long totalExpectedRecords = 0;
  private long totalProcessedRecords = 0;
  private long totalErrorRecords = 0;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDao) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDao = datasetDao;
    loginUserUrl = this.propertiesHolder.metisAuthenticationHost
        + RestEndpoints.AUTHENTICATION_LOGIN;
    startDatasetExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_DATASETID_EXECUTE;
    getWorkflowExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_EXECUTIONS_EXECUTIONID;
    cancelWorkflowExecutionUrl = this.propertiesHolder.metisCoreHost
        + RestEndpoints.ORCHESTRATOR_WORKFLOWS_EXECUTIONS_EXECUTIONID;
    restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
    pathToProcessedDatasetsFile =
        PREFIX_OF_PROCESSED_DATASET_FILE + propertiesHolder.suffixOfProcessedDatasetsLogFile
            + LOG_FILE_EXTENSION;
    switch (propertiesHolder.suffixOfProcessedDatasetsLogFile) {
      case "harvesting":
        processedDatasetsMarker = PropertiesHolder.PROCESSED_DATASETS_HARVESTING;
        break;
      case "preview":
        processedDatasetsMarker = PropertiesHolder.PROCESSED_DATASETS_PREVIEW;
        break;
      case "publish":
        processedDatasetsMarker = PropertiesHolder.PROCESSED_DATASETS_PUBLISH;
        break;
      default:
        processedDatasetsMarker = null;
        throw new IllegalArgumentException(String
            .format("Wrong log file suffix %s", propertiesHolder.suffixOfProcessedDatasetsLogFile));
    }
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
        if (processedDatasetsCounter >= propertiesHolder.numberOfDatasetsToProcess) {
          break;
        }
      }
    } while (nextPage != -1
        && processedDatasetsCounter < propertiesHolder.numberOfDatasetsToProcess);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Total totalExpectedRecords: {}. Total totalProcessedRecords: {}. Total totalErrorRecords: {}.",
        totalExpectedRecords, totalProcessedRecords, totalErrorRecords);
  }

  private void initializeProcessedDatasetList() throws IOException {
    try (Stream<String> stream = Files
        .lines(Paths.get(pathToProcessedDatasetsFile), StandardCharsets.UTF_8)) {
      stream.forEach(line -> processedDatasetIds.add(line));
      if (!processedDatasetIds.isEmpty()) {
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Using Processed datasets file: {}. There are {} datasets that will be bypassed.",
            pathToProcessedDatasetsFile, processedDatasetIds.size());
      }
    }
  }

  private void handleDatasetExecution(Dataset dataset) throws InterruptedException {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Starting datasetId {} execution.",
        dataset.getDatasetId());
    WorkflowExecution workflowExecution = sendDatasetForExecution(dataset.getDatasetId());
    if (workflowExecution == null) {
      LOGGER.info(processedDatasetsMarker,
          dataset.getDatasetId()); //Gets appended and is not timestamp based
      LOGGER.info(PropertiesHolder.FINAL_DATASET_STATUS,
          "FAILED DATASET datasetId: {}",
          dataset.getDatasetId());//Log only the status of the end result
      return;
    }
    dataset.setEcloudDatasetId(
        ExternalRequestUtilMigration.retryableExternalRequest(
            () -> datasetDao.getDatasetByDatasetId(dataset.getDatasetId())).getEcloudDatasetId());
    int previousProcessedRecords = 0;
    int processedRecords;
    final long periodOfNoRecordCountChangeInSeconds = TimeUnit.MINUTES.toSeconds(
        DURATION_OF_NO_RECORD_CHANGE_IN_MINS);
    long stableRecordCountPeriodCounterInSeconds = 0;
    do {
      workflowExecution = monitorAndLog(dataset, workflowExecution.getId());

      processedRecords = workflowExecution.getMetisPlugins().get(0).getExecutionProgress()
          .getProcessedRecords();
      //If we have progress update counters
      if (previousProcessedRecords != processedRecords) {
        stableRecordCountPeriodCounterInSeconds = 0;
        previousProcessedRecords = processedRecords;
      }
      //Request to cancel execution if we haven't had an update for sometime
      stableRecordCountPeriodCounterInSeconds += propertiesHolder.monitorIntervalInSecs;
      if (stableRecordCountPeriodCounterInSeconds >= periodOfNoRecordCountChangeInSeconds) {
        cancelWorkflowExecution(workflowExecution.getId().toString());
        stableRecordCountPeriodCounterInSeconds = 0;
      }

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
    updateLogsAfterExecution(dataset, workflowExecution);
  }

  private WorkflowExecution monitorAndLog(Dataset dataset, ObjectId objectId) {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Requesting WorkflowExecution for datasetId: {} execution.", dataset.getDatasetId());
    WorkflowExecution updatedWorkflowExecution = monitorWorkflowExecution(objectId.toString());
    AbstractMetisPlugin abstractMetisPlugin = updatedWorkflowExecution.getMetisPlugins().get(0);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "WorkflowExecution status info: datasetId: {}, EcloudDatasetId: {}, ExecutionId: {}, PluginType: {}, ExternalTaskId: {}, PluginStatus: {}, ExpectedRecords: {}, ProcessedRecords: {}, ErrorRecords: {}, TaskStatus: {}",
        dataset.getDatasetId(), dataset.getEcloudDatasetId(),
        updatedWorkflowExecution.getId(),
        abstractMetisPlugin.getPluginType(), abstractMetisPlugin.getExternalTaskId(),
        abstractMetisPlugin.getPluginStatus(),
        abstractMetisPlugin.getExecutionProgress().getExpectedRecords(),
        abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
        abstractMetisPlugin.getExecutionProgress().getErrors(),
        abstractMetisPlugin.getExecutionProgress().getStatus());
    totalExpectedRecords += abstractMetisPlugin.getExecutionProgress().getExpectedRecords();
    totalProcessedRecords += abstractMetisPlugin.getExecutionProgress().getProcessedRecords();
    totalErrorRecords += abstractMetisPlugin.getExecutionProgress().getErrors();
    return updatedWorkflowExecution;
  }

  private void updateLogsAfterExecution(Dataset dataset, WorkflowExecution workflowExecution) {
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
    LOGGER.info(processedDatasetsMarker,
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

    try {
      return ExternalRequestUtilMigration.retryableExternalRequest(() -> restTemplate
          .postForObject(builder.buildAndExpand(pathVariables).toUri().toString(),
              new HttpEntity<>(null, accessTokenHeader),
              WorkflowExecution.class, pathVariables));
    } catch (Exception ex) {
      LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Execution of DatasetId {}, failed to start because of remote call exception", datasetId);
    }
    return null;
  }

  private WorkflowExecution monitorWorkflowExecution(String executionId) {
    HttpHeaders accessTokenHeader = new HttpHeaders();
    accessTokenHeader
        .set(AUTHORIZATION, "Bearer " + accessToken);
    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("executionId", executionId);

    return ExternalRequestUtilMigration.retryableExternalRequest(() -> restTemplate.exchange
        (getWorkflowExecutionUrl, HttpMethod.GET, new HttpEntity<>(null, accessTokenHeader),
            WorkflowExecution.class, pathVariables).getBody());
  }

  private void cancelWorkflowExecution(String executionId) {
    HttpHeaders accessTokenHeader = new HttpHeaders();
    accessTokenHeader
        .set(AUTHORIZATION, "Bearer " + accessToken);
    Map<String, String> pathVariables = new HashMap<>();
    pathVariables.put("executionId", executionId);

    ExternalRequestUtilMigration.retryableExternalRequest(() -> restTemplate.exchange
        (cancelWorkflowExecutionUrl, HttpMethod.DELETE, new HttpEntity<>(null, accessTokenHeader),
            Void.class, pathVariables));
  }

  private String loginAndGetAuthorizationToken() {
    UriComponentsBuilder authenticationBuilder = UriComponentsBuilder.fromUriString(loginUserUrl);
    HttpHeaders authenticationHttpHeaders = new HttpHeaders();
    authenticationHttpHeaders.set(AUTHORIZATION, "Basic " + new String(Base64.getEncoder()
        .encode((propertiesHolder.metisUsername + ":" + propertiesHolder.metisPassword).getBytes()),
        Charset.forName("UTF-8")));

    MetisUser metisUser = ExternalRequestUtilMigration.retryableExternalRequest(() -> restTemplate
        .postForObject(authenticationBuilder.build().toUri().toString(),
            new HttpEntity<>(null, authenticationHttpHeaders), MetisUser.class));
    return metisUser.getMetisUserAccessToken().getAccessToken();
  }
}
