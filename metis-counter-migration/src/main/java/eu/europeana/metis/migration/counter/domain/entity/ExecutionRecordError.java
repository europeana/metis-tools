package eu.europeana.metis.migration.counter.domain.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * Entity representing an exception encountered during the processing of an execution record.
 */
@Getter
@Setter
public class ExecutionRecordError {

  private Long id;

  private String datasetId;

  private String message;

  private String exception;
}
