package eu.europeana.metis.migration.counter.domain.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents a warning for an execution record entity.
 */
@Getter
@Setter
public class ExecutionRecordWarning {

  private Long id;

  private ExecutionRecord executionRecord;

  private String message;

  private String exception;
}
