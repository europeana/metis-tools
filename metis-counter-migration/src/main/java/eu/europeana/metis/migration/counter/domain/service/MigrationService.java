package eu.europeana.metis.migration.counter.domain.service;

import eu.europeana.metis.core.common.DaoFieldNames;
import eu.europeana.metis.core.dao.WorkflowExecutionDao.ResultList;
import eu.europeana.metis.core.engine.base.EngineTask;
import eu.europeana.metis.core.engine.base.EngineTaskClient;
import eu.europeana.metis.core.engine.base.EngineTaskSettings;
import eu.europeana.metis.core.engine.base.task.report.EngineTaskProgress;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutionProgress;
import eu.europeana.metis.exception.ExternalTaskException;
import eu.europeana.metis.migration.counter.dao.MetisCoreMongoDao;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * The type Migration service.
 *
 * @param <S> the type parameter
 * @param <T> the type parameter
 */
@Slf4j
@Service
public class MigrationService<S extends EngineTaskSettings, T extends EngineTask> {

  private final EngineTaskClient<S, T> engineTaskClient;

  private final MetisCoreMongoDao metisCoreMongoDao;

  @Value("${mode}")
  private String mode;

  /**
   * Instantiates a new Migration service.
   *
   * @param metisCoreMongoDao the metis core mongo dao
   * @param engineTaskClient the engine task client
   */
  public MigrationService(MetisCoreMongoDao metisCoreMongoDao, EngineTaskClient<S, T> engineTaskClient) {
    this.metisCoreMongoDao = metisCoreMongoDao;
    this.engineTaskClient = engineTaskClient;
  }

  /**
   * Preview list.
   *
   * @param datasetId the dataset id
   * @return the list
   */
  public List<WorkflowExecutionEngineTaskProgress> preview(String datasetId) {
    log.info("Starting migration process...{}", datasetId);
    List<WorkflowExecutionEngineTaskProgress> results = new ArrayList<>();
    final ResultList<WorkflowExecution> allExecutions = metisCoreMongoDao.getWorkflowExecutionDao()
                                                                         .getAllWorkflowExecutions(Set.of(datasetId), null,
                                                                             DaoFieldNames.STARTED_DATE, false, 0,
                                                                             null, false);
    allExecutions.results().forEach(workflowExecution -> {
      workflowExecution.getMetisPlugins().forEach(metisPlugin -> {

        if (metisPlugin instanceof AbstractExecutablePlugin<?>) {
          log.info("Executing metisPlugin {}.", metisPlugin.getClass().getSimpleName());
          ExecutionProgress executionProgressTemp = ((AbstractExecutablePlugin<?>) metisPlugin).getExecutionProgress();
          ExecutionProgress executionProgress = new ExecutionProgress(executionProgressTemp);
          try {
            log.info("Requesting progress information for externalTaskId: {}", ((AbstractExecutablePlugin<?>) metisPlugin).getExternalTaskId());
            EngineTaskProgress engineTaskProgress = engineTaskClient.getEngineTaskProgress(((AbstractExecutablePlugin<?>) metisPlugin).getTopologyName(), ((AbstractExecutablePlugin<?>) metisPlugin).getExternalTaskId(), (((AbstractExecutablePlugin<?>) metisPlugin).getPluginMetadata()).getExecutablePluginType());
            log.info("Task information received for externalTaskId: {}", ((AbstractExecutablePlugin<?>) metisPlugin).getExternalTaskId());
            if (!isProgressEqual(executionProgress, engineTaskProgress)) {
              log.info("Metis & eCloud different");
              logExecutionProgressWithHeader(executionProgress);
              logEngineTaskProgress(engineTaskProgress);
              results.add(new WorkflowExecutionEngineTaskProgress(workflowExecution, metisPlugin, engineTaskProgress));
            }
          } catch (ExternalTaskException e) {
            throw new RuntimeException(e);
          }
        }
      });
    });
    log.info("Ending migration process...{}", datasetId);
    return List.copyOf(results);
  }

  private static void logEngineTaskProgress(EngineTaskProgress engineTaskProgress) {
    log.info("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
        String.format("%25s", "eCloud"),
        String.format("%25d", engineTaskProgress.getExpectedRecords()),
        String.format("%25d", engineTaskProgress.getProcessedRecords()),
        String.format("%25d", engineTaskProgress.getSuccessRecords()),
        String.format("%25d", engineTaskProgress.getFailRecords()),
        String.format("%25d", engineTaskProgress.getWarningRecords()),
        String.format("%25d", engineTaskProgress.getDuplicateRecords()),
        String.format("%25d", engineTaskProgress.getUnchangedRecords()),
        String.format("%25d", engineTaskProgress.getExpectedDepublishRecords()),
        String.format("%25d", engineTaskProgress.getSuccessDepublishRecords()),
        String.format("%25d", engineTaskProgress.getFailDepublishRecords()),
        String.format("%25d", engineTaskProgress.getProcessedDepublishRecords()));
  }

  private static void logExecutionProgressWithHeader(ExecutionProgress executionProgress) {
    log.info("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
        String.format("%25s", "System"),
        String.format("%25s", "ExpectedRecords"),
        String.format("%25s", "ProcessedRecords"),
        String.format("%25s", "SuccessRecords"),
        String.format("%25s", "FailRecords"),
        String.format("%25s", "WarningRecords"),
        String.format("%25s", "DuplicateRecords"),
        String.format("%25s", "UnchangedRecords"),
        String.format("%25s", "ExpectedDepublishRecords"),
        String.format("%25s", "SuccessDepublishRecords"),
        String.format("%25s", "FailDepublishRecords"),
        String.format("%25s", "ProcessedDepublishRecords"));
    log.info("{}", "_".repeat(25 * 12));
    log.info("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
        String.format("%25s", "Metis"),
        String.format("%25d", executionProgress.getExpectedRecords()),
        String.format("%25d", executionProgress.getProcessedRecords()),
        String.format("%25d", executionProgress.getSuccessRecords()),
        String.format("%25d", executionProgress.getFailRecords()),
        String.format("%25d", executionProgress.getWarningRecords()),
        String.format("%25d", executionProgress.getDuplicateRecords()),
        String.format("%25d", executionProgress.getUnchangedRecords()),
        String.format("%25d", executionProgress.getExpectedDepublishRecords()),
        String.format("%25d", executionProgress.getSuccessDepublishRecords()),
        String.format("%25d", executionProgress.getFailDepublishRecords()),
        String.format("%25d", executionProgress.getProcessedDepublishRecords()));
  }

  private boolean isProgressEqual(ExecutionProgress executionProgress, EngineTaskProgress engineTaskProgress) {
    return executionProgress.getExpectedRecords() == engineTaskProgress.getExpectedRecords() &&
        executionProgress.getProcessedRecords() == engineTaskProgress.getProcessedRecords() &&
        executionProgress.getSuccessRecords() == engineTaskProgress.getSuccessRecords() &&
        executionProgress.getFailRecords() == engineTaskProgress.getFailRecords() &&
        executionProgress.getWarningRecords() == engineTaskProgress.getWarningRecords() &&
        executionProgress.getDuplicateRecords() == engineTaskProgress.getDuplicateRecords() &&
        executionProgress.getUnchangedRecords() == engineTaskProgress.getUnchangedRecords() &&
        executionProgress.getExpectedDepublishRecords() == engineTaskProgress.getExpectedDepublishRecords() &&
        executionProgress.getProcessedDepublishRecords() == engineTaskProgress.getProcessedDepublishRecords() &&
        executionProgress.getSuccessDepublishRecords() == engineTaskProgress.getSuccessDepublishRecords() &&
        executionProgress.getFailDepublishRecords() == engineTaskProgress.getFailDepublishRecords();
  }

  /**
   * The type Workflow execution engine task progress.
   */
  public record WorkflowExecutionEngineTaskProgress(WorkflowExecution workflowExecution, AbstractMetisPlugin<?> metisPlugin,
                                                    EngineTaskProgress engineTaskProgress) {

  }

  /**
   * The type Migration counter result.
   */
  public record MigrationCounterResult(ObjectId workflowId, String datasetId, Boolean success) {

  }

  /**
   * Process list.
   *
   * @param workflowExecutionEngineTaskProgressList the workflow execution engine task progress list
   * @return the list
   */
  public List<MigrationCounterResult> process(List<WorkflowExecutionEngineTaskProgress> workflowExecutionEngineTaskProgressList) {
    List<MigrationCounterResult> migrationCounterResults = new ArrayList<>();
    for (WorkflowExecutionEngineTaskProgress item : workflowExecutionEngineTaskProgressList) {
      for (AbstractMetisPlugin<?> metisPlugin : item.workflowExecution.getMetisPlugins()) {
        if (metisPlugin instanceof AbstractExecutablePlugin<?>
            && Objects.equals(metisPlugin.getId(), item.metisPlugin.getId())) {
          log.info("Checking metisPlugin {}.", metisPlugin.getClass().getSimpleName());
          ExecutionProgress executionProgressTemp = ((AbstractExecutablePlugin<?>) metisPlugin).getExecutionProgress();
          ExecutionProgress executionProgress = new ExecutionProgress(executionProgressTemp);
          if (!isProgressEqual(executionProgress, item.engineTaskProgress)) {
            log.info("Updating metisPlugin & eCloud different {}", metisPlugin.getClass().getSimpleName());
            ExecutionProgress updatedExecutionProgress = new ExecutionProgress();
            updatedExecutionProgress.setExpectedRecords(item.engineTaskProgress.getExpectedRecords());
            updatedExecutionProgress.setProcessedRecords(item.engineTaskProgress.getProcessedRecords());
            updatedExecutionProgress.setFailRecords(item.engineTaskProgress.getFailRecords());
            updatedExecutionProgress.setSuccessRecords(item.engineTaskProgress.getSuccessRecords());
            updatedExecutionProgress.setWarningRecords(item.engineTaskProgress.getWarningRecords());
            updatedExecutionProgress.setDuplicateRecords(item.engineTaskProgress.getDuplicateRecords());
            updatedExecutionProgress.setUnchangedRecords(item.engineTaskProgress.getUnchangedRecords());
            updatedExecutionProgress.setExpectedDepublishRecords(item.engineTaskProgress.getExpectedDepublishRecords());
            updatedExecutionProgress.setSuccessDepublishRecords(item.engineTaskProgress.getSuccessDepublishRecords());
            updatedExecutionProgress.setFailDepublishRecords(item.engineTaskProgress.getFailDepublishRecords());
            updatedExecutionProgress.setProcessedDepublishRecords(item.engineTaskProgress.getProcessedDepublishRecords());
            ((AbstractExecutablePlugin<?>) metisPlugin).setExecutionProgress(updatedExecutionProgress);
            logExecutionProgressWithHeader(updatedExecutionProgress);
          }
        }
      }
      try {
        metisCoreMongoDao.getWorkflowExecutionDao().update(item.workflowExecution);
        migrationCounterResults.add(
            new MigrationCounterResult(item.workflowExecution.getId(), item.workflowExecution.getDatasetId(), true));
      } catch (RuntimeException ex) {
        log.error("error updating dataset: {}\r\n{}", item.workflowExecution().getDatasetId(), ex);
        migrationCounterResults.add(
            new MigrationCounterResult(item.workflowExecution.getId(), item.workflowExecution.getDatasetId(), false));
      }
    }
    return migrationCounterResults;
  }

}
