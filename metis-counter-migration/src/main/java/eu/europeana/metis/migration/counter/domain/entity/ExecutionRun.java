package eu.europeana.metis.migration.counter.domain.entity;

import eu.europeana.metis.migration.counter.config.BatchStatus;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExecutionRun {

  private Long id;

  private String sourceExecutionId;

  private String datasetId;

  private String executionId;

  private String executionName;

  private BatchStatus batchStatus;
}
