package eu.europeana.metis.migration.counter.config;

import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.migration.counter.config.WorkflowConfigurationProperties.ParallelizeConfig;
import eu.europeana.metis.migration.counter.dao.MetisCoreMongoDao;
import eu.europeana.metis.migration.counter.domain.dto.AbstractExecutionRecordDTO;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Future;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.data.MongoPagingItemReader;
import org.springframework.batch.infrastructure.item.data.builder.MongoPagingItemReaderBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * The type Preview job config.
 */
@Configuration
public class PreviewJobConfig {
  private static final String STEP_FETCH_DATASET_INFO = "fetch-dataset-info";
  private final ParallelizeConfig parallelizeConfig;

  /**
   * Instantiates a new Preview job config.
   *
   * @param workflowConfigurationProperties the workflow configuration properties
   */
  PreviewJobConfig(WorkflowConfigurationProperties workflowConfigurationProperties) {
    this.parallelizeConfig = workflowConfigurationProperties.workflow().get("DATASET");
  }

  /**
   * Dataset batch job job.
   *
   * @param jobRepository the job repository
   * @param fetchDatasetInfoStep the fetch dataset info step
   * @return the job
   */
  @Bean
  Job datasetBatchJob(JobRepository jobRepository,
      @Qualifier(STEP_FETCH_DATASET_INFO) Step fetchDatasetInfoStep) {
    return new JobBuilder("jobDataset"+ Instant.now(), jobRepository)
        .start(fetchDatasetInfoStep)
        .build();
  }

  /**
   * Dataset mongo paging item reader mongo paging item reader.
   *
   * @param mongoTemplate the mongo template
   * @return the mongo paging item reader
   */
  @Bean
  MongoPagingItemReader<Dataset> datasetMongoPagingItemReader(MongoTemplate mongoTemplate) {
    return new MongoPagingItemReaderBuilder<Dataset>()
        .template(mongoTemplate)
        .name("datasetMongoPagingItemReader")
        .collection("Dataset")
        //.jsonQuery("{datasetId:\"1543\"}")
        .jsonQuery("{}")
        .targetType(Dataset.class)
        .sorts(Map.of("datasetId", Direction.ASC))
        .pageSize(10)
        .build();
  }
  /**
   * Fetch dataset info step step.
   *
   * @param jobRepository the job repository
   * @param datasetMongoPagingItemReader the dataset mongo paging item reader
   * @param datasetAsyncItemProcessor the dataset async item processor
   * @param executionRecordDTOAsyncItemWriter the execution record dto async item writer
   * @param mongoTransactionManager the mongo transaction manager
   * @return the step
   */
  @Bean(STEP_FETCH_DATASET_INFO)
  Step fetchDatasetInfoStep(JobRepository jobRepository,
      MongoPagingItemReader<Dataset> datasetMongoPagingItemReader,
      ItemProcessor<Dataset, Future<AbstractExecutionRecordDTO>> datasetAsyncItemProcessor,
      ItemWriter<Future<AbstractExecutionRecordDTO>> executionRecordDTOAsyncItemWriter,
      MongoTransactionManager mongoTransactionManager) {
    return new StepBuilder(STEP_FETCH_DATASET_INFO, jobRepository)
        .<Dataset, Future<AbstractExecutionRecordDTO>>chunk(parallelizeConfig.parallelizeSize())
        .transactionManager(mongoTransactionManager)
        .reader(datasetMongoPagingItemReader)
        .processor(datasetAsyncItemProcessor)
        .writer(executionRecordDTOAsyncItemWriter)
        .build();
  }

  /**
   * Dataset async item processor item processor.
   *
   * @param datasetItemProcessor the dataset item processor
   * @param datasetStepAsyncTaskExecutor the dataset step async task executor
   * @return the item processor
   */
  @Bean("datasetAsyncItemProcessor")
  ItemProcessor<Dataset, Future<AbstractExecutionRecordDTO>> datasetAsyncItemProcessor(
      @Qualifier("datasetItemProcessor") ItemProcessor<Dataset, AbstractExecutionRecordDTO> datasetItemProcessor,
      @Qualifier("datasetStepAsyncTaskExecutor") TaskExecutor datasetStepAsyncTaskExecutor) {
    AsyncItemProcessor<Dataset, AbstractExecutionRecordDTO> asyncItemProcessor
        = new AsyncItemProcessor<>(datasetItemProcessor);
    asyncItemProcessor.setTaskExecutor(datasetStepAsyncTaskExecutor);
    return asyncItemProcessor;
  }

  /**
   * Dataset step async task executor task executor.
   *
   * @return the task executor
   */
  @Bean(name = "datasetStepAsyncTaskExecutor")
  TaskExecutor datasetStepAsyncTaskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("dataset" + "-");
    executor.setCorePoolSize(parallelizeConfig.parallelizeSize());
    executor.setMaxPoolSize(parallelizeConfig.parallelizeSize());
    executor.initialize();
    return executor;
  }

}
