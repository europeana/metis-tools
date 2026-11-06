package eu.europeana.metis.migration.counter.domain.processor;


import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.migration.counter.config.BatchStatus;
import eu.europeana.metis.migration.counter.config.Mode;
import eu.europeana.metis.migration.counter.domain.dto.AbstractExecutionRecordDTO;
import eu.europeana.metis.migration.counter.domain.dto.ExceptionInfoDTO;
import eu.europeana.metis.migration.counter.domain.dto.ExecutionRecordConverter;
import eu.europeana.metis.migration.counter.domain.dto.FailExecutionRecordDTO;
import eu.europeana.metis.migration.counter.domain.dto.JobMetadataDTO;
import eu.europeana.metis.migration.counter.domain.dto.SuccessExecutionRecordDTO;
import eu.europeana.metis.migration.counter.domain.entity.ExecutionRecord;
import eu.europeana.metis.migration.counter.domain.entity.ExecutionRun;
import java.time.Instant;
import java.util.function.BiFunction;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base class for processing ExecutionRecord objects into ExecutionRecordDTO objects.
 *
 * <p>Provides a framework for handling item processing with error handling.
 * <p>Extends the functionality of AbstractMetisItemProcessor with specific handling for execution records.
 * <p>This class defines a standardized process flow, converting entities, adding metadata, and handling exceptions.
 */
public abstract class AbstractExecutionRecordMetisItemProcessor extends
    AbstractMetisItemProcessor<Dataset, JobMetadataDTO, AbstractExecutionRecordDTO> {

  @Override
  public AbstractExecutionRecordDTO process(@NotNull Dataset dataset) {

    ExecutionRecord executionRecord1 = new ExecutionRecord();
    executionRecord1.setDatasetId(dataset.getDatasetId());
    executionRecord1.setDataset(dataset);
    executionRecord1.setId(Instant.now().toEpochMilli());
    executionRecord1.setRecordData(Instant.now().toString());

    ExecutionRun executionRun = new ExecutionRun();
    executionRun.setDatasetId(dataset.getDatasetId());
    if(Mode.valueOf(getMode()).equals(Mode.PRE_PROCESS)) {
      executionRun.setBatchStatus(BatchStatus.PREVIEWED);
    } else if (Mode.valueOf(getMode()).equals(Mode.DEFAULT)) {
      executionRun.setBatchStatus(BatchStatus.PROCESSED);
    }
    executionRecord1.setExecutionRun(executionRun);

    SuccessExecutionRecordDTO originSuccessExecutionRecordDTO =
        ExecutionRecordConverter.convertToExecutionRecordDTO(executionRecord1);

    JobMetadataDTO jobMetadataDTO = JobMetadataDTO.builder()
                                                  .successExecutionRecordDTO(originSuccessExecutionRecordDTO)
                                                  .targetExecutionName(getExecutionName())
                                                  .targetExecutionId(getTargetExecutionId()).build();

    return processCapturingException(jobMetadataDTO, getProcessRecordFunction(), defaultHandler()
    );
  }

  /**
   * Provides a default error handling function for transforming failed job metadata into a failure execution record DTO.
   *
   * <p>The returned function takes a JobMetadataDTO and an Exception and produces a FailExecutionRecordDTO with details from
   * the provided metadata and exception.
   *
   * @return A BiFunction that converts JobMetadataDTO and Exception into a FailExecutionRecordDTO.
   */
  public static BiFunction<JobMetadataDTO, Exception, AbstractExecutionRecordDTO> defaultHandler() {
    return (jobMetadataDTO, exception) -> FailExecutionRecordDTO.createValidated(
        builder -> builder
            .datasetId(jobMetadataDTO.getSuccessExecutionRecordDTO().getDatasetId())
            .recordId(jobMetadataDTO.getSuccessExecutionRecordDTO().getRecordId())
            .externalRecordId(jobMetadataDTO.getSuccessExecutionRecordDTO().getExternalRecordId())
            .sourceRecordId(jobMetadataDTO.getSuccessExecutionRecordDTO().getSourceRecordId())
            .executionId(jobMetadataDTO.getTargetExecutionId())
            .executionName(jobMetadataDTO.getTargetExecutionName())
            .exceptionInfoDTO(ExceptionInfoDTO.from(exception))
    );
  }

}
