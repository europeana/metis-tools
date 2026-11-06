package eu.europeana.metis.migration.counter.domain.dto;

import eu.europeana.metis.migration.counter.domain.entity.ExecutionRecord;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

/**
 * Utility class for converting ExecutionRecord and DTO objects.
 */
@UtilityClass
public final class ExecutionRecordConverter {

  /**
   * Converts an ExecutionRecord entity to a SuccessExecutionRecordDTO object.
   *
   * @param executionRecord The ExecutionRecord to be converted.
   * @return The corresponding SuccessExecutionRecordDTO object.
   */
  public static SuccessExecutionRecordDTO convertToExecutionRecordDTO(ExecutionRecord executionRecord) {
    Set<ExceptionInfoDTO> exceptionInfoDTOs = executionRecord.getExecutionRecordWarning().stream()
                                                             .map(executionRecordWarning -> new ExceptionInfoDTO(
                                                                 executionRecordWarning.getMessage(),
                                                                 executionRecordWarning.getException()))
                                                             .collect(Collectors.toSet());
    return SuccessExecutionRecordDTO.createValidated(builder -> builder
        .datasetId(executionRecord.getExecutionRun().getDatasetId())
        .dataset(executionRecord.getDataset())
        .differences(executionRecord.getDifferences())
        .executionId(executionRecord.getExecutionRun().getExecutionId()+Instant.now().toString())
        .externalRecordId("externalRecordId"+Instant.now().toString())
        .recordId("recordId"+Instant.now().toString())
        .sourceRecordId("sourceRecordId"+Instant.now().toString())
        .executionName(executionRecord.getExecutionRun().getExecutionName()+Instant.now().toString())
        .recordData(executionRecord.getRecordData())
        .batchStatus(executionRecord.getExecutionRun().getBatchStatus())
        .exceptionWarnings(exceptionInfoDTOs));
  }

//  /**
  //   * Converts a SuccessExecutionRecordDTO object to an ExecutionRecord object.
  //   *
  //   * @param executionRecordDTO The SuccessExecutionRecordDTO object containing execution record data to be converted.
  //   * @param executionRun the execution run for which the record is created.
  //   * @return The converted ExecutionRecord object.
  //   */
  //  public static ExecutionRecord convertToExecutionRecord(SuccessExecutionRecordDTO executionRecordDTO, ExecutionRun executionRun) {
  //    final ExecutionRecord executionRecord = new ExecutionRecord();
  //    executionRecord.setExecutionRun(executionRun);
  //
  //    ExecutionRecordIdentifier executionRecordIdentifier = new ExecutionRecordIdentifier();
  //    executionRecordIdentifier.setExternalRecordId(executionRecordDTO.getExternalRecordId());
  //    executionRecordIdentifier.setSourceRecordId(executionRecordDTO.getSourceRecordId());
  //    executionRecordIdentifier.setRecordId(executionRecordDTO.getRecordId());
  //    executionRecord.setIdentifier(executionRecordIdentifier);
  //
  //    executionRecord.setRecordData(executionRecordDTO.getRecordData());
  //
  //    List<ExecutionRecordWarning> executionRecordWarnings = new ArrayList<>();
  //    for (ExceptionInfoDTO exceptionInfoDTO : executionRecordDTO.getExceptionWarnings()) {
  //      ExecutionRecordWarning executionRecordWarning = new ExecutionRecordWarning();
  //      executionRecordWarning.setMessage(exceptionInfoDTO.getMessage());
  //      executionRecordWarning.setException(exceptionInfoDTO.getStackTrace());
  //      executionRecordWarning.setExecutionRecord(executionRecord);
  //      executionRecordWarnings.add(executionRecordWarning);
  //    }
  //    executionRecord.setExecutionRecordWarning(executionRecordWarnings);
  //    return executionRecord;
  //  }
  //
  //  /**
  //   * Constructs an ExecutionRecordIdentifierKey object based on the provided ExecutionRecordDTO.
  //   *
  //   * @param executionRecordDTO The ExecutionRecordDTO containing the data to populate the ExecutionRecordIdentifierKey.
  //   * @return A populated ExecutionRecordIdentifierKey instance derived from the input ExecutionRecordDTO.
  //   */
  //  private static @NotNull ExecutionRun getExecution(
  //      AbstractExecutionRecordDTO executionRecordDTO) {
  //    ExecutionRun executionRun = new ExecutionRun();
  //    executionRun.setDatasetId(executionRecordDTO.getDatasetId());
  //    executionRun.setExecutionId(executionRecordDTO.getExecutionId());
  //    executionRun.setExecutionName(executionRecordDTO.getExecutionName());
  //    return executionRun;
  //  }
  //
  //  /**
  //   * Converts a FailExecutionRecordDTO object into an ExecutionRecordException entity.
  //   *
  //   * @param failExecutionRecordDTO The FailExecutionRecordDTO containing failure details to be converted.
  //   * @param executionRun the execution run for which the record is created.
  //   * @return An ExecutionRecordException entity reflecting the input data.
  //   */
  //  public static ExecutionRecordError converterToExecutionRecordError(FailExecutionRecordDTO failExecutionRecordDTO,
  //      ExecutionRun executionRun) {
  //
  //    final ExecutionRecordError executionRecordError = new ExecutionRecordError();
  //    executionRecordError.setExecutionRun(executionRun);
  //
  //    ExecutionRecordIdentifier executionRecordIdentifier = new ExecutionRecordIdentifier();
  //    executionRecordIdentifier.setExternalRecordId(failExecutionRecordDTO.getExternalRecordId());
  //    executionRecordIdentifier.setSourceRecordId(failExecutionRecordDTO.getSourceRecordId());
  //    executionRecordIdentifier.setRecordId(failExecutionRecordDTO.getRecordId());
  //    executionRecordError.setIdentifier(executionRecordIdentifier);
  //
  //    if (failExecutionRecordDTO.getExceptionInfoDTO() != null) {
  //      executionRecordError.setMessage(failExecutionRecordDTO.getExceptionInfoDTO().getMessage());
  //      executionRecordError.setException(failExecutionRecordDTO.getExceptionInfoDTO().getStackTrace());
  //    }
  //
  //    return executionRecordError;
  //  }
}
