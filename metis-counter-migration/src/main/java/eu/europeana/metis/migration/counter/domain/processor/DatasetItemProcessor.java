package eu.europeana.metis.migration.counter.domain.processor;

import static eu.europeana.metis.migration.counter.domain.dto.SuccessExecutionRecordDTO.createCopyIdentifiersValidated;

import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.migration.counter.config.BatchStatus;
import eu.europeana.metis.migration.counter.config.Mode;
import eu.europeana.metis.migration.counter.domain.dto.AbstractExecutionRecordDTO;
import eu.europeana.metis.migration.counter.domain.dto.JobMetadataDTO;
import eu.europeana.metis.migration.counter.domain.dto.SuccessExecutionRecordDTO;
import eu.europeana.metis.migration.counter.domain.service.MigrationService;
import eu.europeana.metis.migration.counter.domain.service.MigrationService.MigrationCounterResult;
import eu.europeana.metis.migration.counter.domain.service.MigrationService.WorkflowExecutionEngineTaskProgress;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.function.ThrowingFunction;

/**
 * Processor responsible for dataset.
 */
@StepScope
@Component("datasetItemProcessor")
@Slf4j
public class DatasetItemProcessor extends AbstractExecutionRecordMetisItemProcessor {

  private final MigrationService migrationService;
  @Value("${mode}")
  private String mode;

  /**
   * Constructor with service parameter.
   *
   * @param migrationService The service responsible for validating record data.
   */
  public DatasetItemProcessor(MigrationService migrationService) {
    this.migrationService = migrationService;
  }

  @Override
  public ThrowingFunction<JobMetadataDTO, AbstractExecutionRecordDTO> getProcessRecordFunction() {
    return jobMetadataDTO -> {
      SuccessExecutionRecordDTO originSuccessExecutionRecordDTO = jobMetadataDTO.getSuccessExecutionRecordDTO();
      Dataset dataset = originSuccessExecutionRecordDTO.getDataset();

      if (Mode.valueOf(mode).equals(Mode.PRE_PROCESS) || Mode.valueOf(mode).equals(Mode.DEFAULT)) {
        List<WorkflowExecutionEngineTaskProgress> differences  = migrationService.preview(dataset.getDatasetId());
        if (Mode.valueOf(mode).equals(Mode.DEFAULT) && !differences.isEmpty()) {
          List<MigrationCounterResult> migrationCounterResults = migrationService.process(differences);
          log.info("Migration results\r\nsuccess:{}\tfailed:{}",
              migrationCounterResults.stream().filter(MigrationCounterResult::success).count(),
              migrationCounterResults.stream().filter( item-> !item.success()).count());
        }
      }

      return createCopyIdentifiersValidated(
          originSuccessExecutionRecordDTO,
          jobMetadataDTO.getTargetExecutionId(),
          jobMetadataDTO.getTargetExecutionName(),
          builder -> builder
                  .recordData(originSuccessExecutionRecordDTO.getRecordData())
              .batchStatus(BatchStatus.PREVIEWED)
              .dataset(originSuccessExecutionRecordDTO.getDataset())
      );
    };
  }
}
