package eu.europeana.metis.migration.counter.domain.writer;

import eu.europeana.metis.migration.counter.domain.dto.AbstractExecutionRecordDTO;
import org.springframework.batch.infrastructure.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

/**
 * The type Execution dataset dto item writer.
 */
@Component
public class ExecutionDatasetDTOItemWriter extends MongoItemWriter<AbstractExecutionRecordDTO> {

  /**
   * Create a new instance of {@link MongoItemWriter} with the provided {@link MongoOperations} template. The template is
   * required.
   *
   * @param template the template implementation to be used. Must not be null.
   * @since 6.0
   */
  public ExecutionDatasetDTOItemWriter(MongoOperations template) {
    super(template);
  }
}
