package eu.europeana.metis.migration.results.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MigrationResult {

  private static final Set<String> IGNORE_HARVEST_ERRORS_DATASETS = new HashSet<>(Arrays.asList(
      "0940408", "0940414", "0940429", "0940431", "0940433", "0940435", "0940442"
  ));

  public enum ResultStatus implements Predicate<MigrationResult> {
    EMPTY, DID_NOT_END_NORMALLY, COMPLETED_WITH_ERRORS, TOTALS_DONT_MATCH, SUCCESS;

    @Override
    public boolean test(MigrationResult migrationResult) {
      return migrationResult != null && this == migrationResult.computeStatus();
    }
  }

  private static final String DATASET_ID_GROUP = "datasetId";
  private static final String ECLOUD_DATASET_ID_GROUP = "ecloudDatasetId";
  private static final String EXECUTION_ID_GROUP = "executionId";
  private static final String PLUGIN_TYPE_GROUP = "pluginType";
  private static final String EXTERNAL_TASK_ID_GROUP = "externalTaskId";
  private static final String PLUGIN_STATUS_GROUP = "pluginStatus";
  private static final String EXPECTED_RECORDS_GROUP = "expectedRecords";
  private static final String PROCESSED_RECORDS_GROUP = "processedRecords";
  private static final String ERROR_RECORDS_GROUP = "errorRecords";
  private static final String TASK_STATUS_GROUP = "taskStatus";

  private static final String LINE_TEMPLATE =
      ".*?- datasetId: (?<" + DATASET_ID_GROUP + ">[^,].*)"
          + ", EcloudDatasetId: (?<" + ECLOUD_DATASET_ID_GROUP + ">[^,].*)"
          + ", ExecutionId: (?<" + EXECUTION_ID_GROUP + ">[^,].*)"
          + ", PluginType: (?<" + PLUGIN_TYPE_GROUP + ">[^,].*)"
          + ", ExternalTaskId: (?<" + EXTERNAL_TASK_ID_GROUP + ">[^,].*)"
          + ", PluginStatus: (?<" + PLUGIN_STATUS_GROUP + ">[^,].*)"
          + ", ExpectedRecords: (?<" + EXPECTED_RECORDS_GROUP + ">[^,].*)"
          + ", ProcessedRecords: (?<" + PROCESSED_RECORDS_GROUP + ">[^,].*)"
          + ", ErrorRecords: (?<" + ERROR_RECORDS_GROUP + ">[^,].*)"
          + ", TaskStatus: (?<" + TASK_STATUS_GROUP + ">[^,].*)"
          + ".*";
  private static final Pattern LINE_PATTERN = Pattern.compile(LINE_TEMPLATE);

  private final String runId;
  private final DatasetInfo datasetInfo;

  private final String datasetId;
  private final String ecloudDatasetId;
  private final String executionId;
  private final PluginType pluginType;
  private final String externalTaskId;
  private final PluginStatus pluginStatus;
  private final Integer expectedRecords;
  private final Integer processedRecords;
  private final Integer errorRecords;
  private final TaskStatus taskStatus;

  public MigrationResult(String runId, DatasetInfo datasetInfo, String datasetId,
      String ecloudDatasetId, String executionId, PluginType pluginType, String externalTaskId,
      PluginStatus pluginStatus, Integer expectedRecords, Integer processedRecords,
      Integer errorRecords, TaskStatus taskStatus) {
    this.runId = runId;
    this.datasetInfo = datasetInfo;
    this.datasetId = datasetId;
    this.ecloudDatasetId = ecloudDatasetId;
    this.executionId = executionId;
    this.pluginType = pluginType;
    this.externalTaskId = externalTaskId;
    this.pluginStatus = pluginStatus;
    this.expectedRecords = expectedRecords;
    this.processedRecords = processedRecords;
    this.errorRecords = errorRecords;
    this.taskStatus = taskStatus;
  }

  public MigrationResult(String runId, Function<String, DatasetInfo> datasetInfoSupplier,
      String line) {

    // Check line format
    final Matcher matcher = LINE_PATTERN.matcher(line);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Line does not match pattern: " + line);
    }

    // Save the run ID
    this.runId = runId;

    // Get the strings
    datasetId = matcher.group(DATASET_ID_GROUP).trim();
    ecloudDatasetId = matcher.group(ECLOUD_DATASET_ID_GROUP).trim();
    executionId = matcher.group(EXECUTION_ID_GROUP).trim();
    externalTaskId = matcher.group(EXTERNAL_TASK_ID_GROUP).trim();
    if (datasetId.isEmpty() || ecloudDatasetId.isEmpty() || executionId.isEmpty() || externalTaskId
        .isEmpty()) {
      throw new IllegalArgumentException("One of the string variables is empty: " + line);
    }

    // Get the enums
    pluginType = Enum.valueOf(PluginType.class, matcher.group(PLUGIN_TYPE_GROUP).trim());
    pluginStatus = Enum.valueOf(PluginStatus.class, matcher.group(PLUGIN_STATUS_GROUP).trim());
    final String taskStatusString = matcher.group(TASK_STATUS_GROUP).trim();
    taskStatus =
        "null".equals(taskStatusString) ? null : Enum.valueOf(TaskStatus.class, taskStatusString);

    // Get integers
    expectedRecords = parseInteger(matcher.group(EXPECTED_RECORDS_GROUP));
    processedRecords = parseInteger(matcher.group(PROCESSED_RECORDS_GROUP));
    errorRecords = parseInteger(matcher.group(ERROR_RECORDS_GROUP));

    // Set the dataset info
    datasetInfo = datasetInfoSupplier.apply(datasetId);
  }

  private static Integer parseInteger(String numberString) {
    if ("UNKNOWN".equals(numberString.trim())) {
      return null;
    }
    int result = Integer.parseInt(numberString);
    if (result == -1) {
      return null;
    }
    return result;
  }

  private ResultStatus computeStatus() {

    // Determine if errors occurred according to the counters.
    final boolean errorsOccurred = errorRecords != null && errorRecords > 0;

    // First check that the dataset was not empty.
    if (processedRecords != null && processedRecords == 0
        && (expectedRecords == null || expectedRecords == 0)
        && !errorsOccurred
        && pluginStatus != PluginStatus.CANCELLED) {
      return ResultStatus.EMPTY;
    }

    // So the dataset was not empty. Check if we ignore errors.
    if (pluginType == PluginType.OAIPMH_HARVEST && IGNORE_HARVEST_ERRORS_DATASETS
        .contains(datasetId)) {
      return ResultStatus.SUCCESS;
    }

    // Then check whether the non-empty dataset ended normally.
    if (pluginStatus != PluginStatus.FINISHED || taskStatus != TaskStatus.PROCESSED) {
      return ResultStatus.DID_NOT_END_NORMALLY;
    }

    // So the non-empty dataset ended normally. Check if errors occurred.
    if (errorsOccurred) {
      return ResultStatus.COMPLETED_WITH_ERRORS;
    }

    // So everything went well. Check if the totals match.
    if (expectedRecords == null || processedRecords == null || errorRecords == null
        || !expectedRecords.equals(processedRecords)) {
      return ResultStatus.TOTALS_DONT_MATCH;
    }

    // So all is well.
    return ResultStatus.SUCCESS;
  }

  // Whether the run was successful (i.e. completed normally, no missing records or errors).
  public boolean wasSuccessful() {
    final boolean proceedInCaseOfErrors =
        pluginType == PluginType.OAIPMH_HARVEST && IGNORE_HARVEST_ERRORS_DATASETS
            .contains(datasetId);
    final ResultStatus status = computeStatus();
    return proceedInCaseOfErrors || ResultStatus.EMPTY == status || ResultStatus.SUCCESS == status;
  }

  // Determines whether this set should be run again (regardless of whether it was successful).
  public boolean shouldDoAgain() {
    // For now, we don't ever redo a result. Later this should change to mean pretty much the same
    // as wasSuccessful.
    return false;
  }

  // Whether this dataset can go to the next plugin: was it successful, non-empty and definitive?
  public boolean readyForNextPlugin() {
    return wasSuccessful() && computeStatus() != ResultStatus.EMPTY && !shouldDoAgain();
  }

  public String toSummaryWithStatus() {
    return String
        .format("    * Dataset: %s, Run %s, Plugin status: %s, Task status: %s", datasetId, runId,
            pluginStatus, taskStatus);
  }

  public String toSummaryWithCounts() {
    return String
        .format(
            "    * Dataset: %s, Run %s, Expected records: %d, Processed records: %d, Errors: %d",
            datasetId, runId, expectedRecords, processedRecords, errorRecords);
  }

  public String toSummaryForEcloud() {
    return "MigrationResult{" +
        "datasetId='" + datasetId + '\'' +
        ", ecloudDatasetId='" + ecloudDatasetId + '\'' +
        ", executionId='" + executionId + '\'' +
        ", externalTaskId='" + externalTaskId + '\'' +
        ", expectedRecords=" + expectedRecords +
        ", processedRecords=" + processedRecords +
        ", errorRecords=" + errorRecords +
        ", taskStatus=" + taskStatus +
        '}';
  }

  public String toString() {
    return "MigrationResult{" +
        "runId='" + runId + '\'' +
        ", datasetId='" + datasetId + '\'' +
        ", ecloudDatasetId='" + ecloudDatasetId + '\'' +
        ", executionId='" + executionId + '\'' +
        ", pluginType=" + pluginType +
        ", externalTaskId='" + externalTaskId + '\'' +
        ", pluginStatus=" + pluginStatus +
        ", expectedRecords=" + expectedRecords +
        ", processedRecords=" + processedRecords +
        ", errorRecords=" + errorRecords +
        ", taskStatus=" + taskStatus +
        ", datasetInfo.nameInCsv='" + datasetInfo.getNameInCsv() + '\'' +
        ", datasetInfo.nameInDb='" + datasetInfo.getNameInDb() + '\'' +
        ", datasetInfo.stateInCsv='" + datasetInfo.getStateInCsv() + '\'' +
        '}';
  }

  public String getRunId() {
    return runId;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public String getEcloudDatasetId() {
    return ecloudDatasetId;
  }

  public String getExecutionId() {
    return executionId;
  }

  public PluginType getPluginType() {
    return pluginType;
  }

  public String getExternalTaskId() {
    return externalTaskId;
  }

  public PluginStatus getPluginStatus() {
    return pluginStatus;
  }

  public Integer getExpectedRecords() {
    return expectedRecords;
  }

  public Integer getProcessedRecords() {
    return processedRecords;
  }

  public Integer getErrorRecords() {
    return errorRecords;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  public DatasetInfo getDatasetInfo() {
    return datasetInfo;
  }
}
