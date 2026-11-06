package eu.europeana.metis.migration.counter.domain.entity;

import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.migration.counter.config.BatchStatus;
import eu.europeana.metis.migration.counter.domain.service.MigrationService.WorkflowExecutionEngineTaskProgress;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents the main execution record entity.
 */
@Getter
@Setter
public class ExecutionRecord {

  private Long id;
  private ExecutionRun executionRun;
  private String datasetId;
  private Dataset dataset;
  private String recordData;
  private List<WorkflowExecutionEngineTaskProgress> differences = new ArrayList<>();
  private List<ExecutionRecordWarning> executionRecordWarning = new ArrayList<>();
}

