package eu.europeana.metis.migration.counter.config;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.migration.counter.dao.MetisCoreMongoDao;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableConfigurationProperties({WorkflowConfigurationProperties.class})
@Slf4j
public class MigrationCounterConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationCounterConfiguration.class);

  private static final int WORKFLOW_CORE_POOL_SIZE = 4;
  private static final int WORKFLOW_QUEUE_CAPACITY = 20;
  public static final int HTTP_CLIENT_TIMEOUT_SECONDS = 5;

  private final PropertiesHolder propertiesHolder;

  public MigrationCounterConfiguration(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
  }

  @Bean
  MongoInitializer mongoInitializerMetisCore()
      throws CustomTruststoreAppender.TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolder.getTruststorePath())
        && StringUtils.isNotEmpty(propertiesHolder.getTruststorePassword())) {
      LOGGER.info("Append default truststore with custom truststore");
      CustomTruststoreAppender.appendCustomTruststoreToDefault(propertiesHolder.getTruststorePath(),
          propertiesHolder.getTruststorePassword());
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  @Bean
  MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
    return new MongoTransactionManager(dbFactory);
  }

  @Bean
  MongoClient mongoClient(MongoInitializer mongoInitializer) {
    return mongoInitializer.getMongoClient();
  }

  @Bean
  MetisCoreMongoDao metisCoreMongoDao(MongoClient mongoClient, PropertiesHolder propertiesHolder) {
    return new MetisCoreMongoDao(mongoClient, propertiesHolder);
  }

  @Bean
  MongoTemplate mongoTemplate(MongoClient mongoClient) {
    return new MongoTemplate(mongoClient, propertiesHolder.getMetisCoreMongoDb());
  }

  @Bean(name = "pipelineTaskExecutor")
  TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(WORKFLOW_CORE_POOL_SIZE);
    executor.setMaxPoolSize(WORKFLOW_CORE_POOL_SIZE);
    executor.setQueueCapacity(WORKFLOW_QUEUE_CAPACITY);
    executor.setThreadNamePrefix("PipelineJob-");
    executor.initialize();
    return executor;
  }

  //  @Bean
  //  public CommandLineRunner commandLineRunner(
  //      @Qualifier("pipelineTaskExecutor") TaskExecutor pipelineTaskExecutor,
  //      JobOperator jobOperator,
  //      Job job,
  //      ApplicationContext applicationContext) {
  //      return args -> {
  //
  //      pipelineTaskExecutor.execute(() -> {
  //
  //          try {
  //            JobParameter<String> jobParameter = new JobParameter<>("executionId", "executionId" + Instant.now().toEpochMilli(),
  //                String.class);
  //            JobParameters jobParameters = new JobParameters(Set.of(jobParameter));
  //
  //           JobExecution execution = jobOperator.start(job, jobParameters);
  //            log.info("Job execution with id {}::{}", execution.getId(), execution.getStatus());
  //
  //          } catch (JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException | InvalidJobParametersException |
  //                   JobRestartException e) {
  //            e.printStackTrace();
  //
  //          }
  //
  //        });
  //      };
  //  }
}
