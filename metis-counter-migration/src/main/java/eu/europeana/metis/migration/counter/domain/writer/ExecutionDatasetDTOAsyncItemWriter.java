package eu.europeana.metis.migration.counter.domain.writer;

import eu.europeana.metis.migration.counter.domain.dto.AbstractExecutionRecordDTO;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.stereotype.Component;

/**
 * Asynchronous ItemWriter implementation for handling {@link AbstractExecutionRecordDTO} objects.
 *
 * <p>Delegates the writing of {@link AbstractExecutionRecordDTO} objects to an instance of
 * {@link ExecutionDatasetDTOItemWriter}.
 */
@Component
public class ExecutionDatasetDTOAsyncItemWriter extends AsyncItemWriter<AbstractExecutionRecordDTO> {

  private final ExecutionDatasetDTOItemWriter executionDatasetDTOItemWriter;

  /**
   * Constructs an instance of ExecutionRecordDTOAsyncItemWriter by delegating to the provided
   * {@link ExecutionDatasetDTOItemWriter}.
   *
   * @param executionDatasetDTOItemWriter An instance of {@link ExecutionDatasetDTOItemWriter} used to perform the actual writing of
   * {@link AbstractExecutionRecordDTO} objects.
   */
  public ExecutionDatasetDTOAsyncItemWriter(ExecutionDatasetDTOItemWriter executionDatasetDTOItemWriter) {
    super(executionDatasetDTOItemWriter);
    this.executionDatasetDTOItemWriter = executionDatasetDTOItemWriter;
  }

  /**
   * Performs pre-step initialization for the {@link ExecutionDatasetDTOItemWriter}.
   */
  @BeforeStep
  public void beforeStep() {

  }
}
