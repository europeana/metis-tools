package eu.europeana.metis.tombstone.config;

import eu.europeana.metis.tombstone.batch.JobCompletionNotificationListener;
import eu.europeana.metis.tombstone.batch.TombstoneEntry;
import eu.europeana.metis.tombstone.batch.TombstoneItemProcessor;
import javax.sql.DataSource;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
@EnableConfigurationProperties({AppConfiguration.class})
public class BatchConfiguration {

  private final AppConfiguration appConfiguration;

  @Autowired
  public BatchConfiguration(AppConfiguration appConfiguration) {
    this.appConfiguration = appConfiguration;
  }

  @Bean
  public FlatFileItemReader<TombstoneEntry> reader() {
    return new FlatFileItemReaderBuilder<TombstoneEntry>()
        .name("TomstoneEntryItemReader")
        .resource(new ClassPathResource("tombstone-data.csv"))
        .delimited()
        .strict(false)
        .names("europeanaId", "resourceUrl", "status")
        .targetType(TombstoneEntry.class)
        .build();
  }

  @Bean
  public TombstoneItemProcessor processor() {
    return new TombstoneItemProcessor();
  }

  @Bean
  public JdbcBatchItemWriter<TombstoneEntry> writer(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<TombstoneEntry>()
        .sql("INSERT INTO entry (europeana_id, resource_url, status) VALUES (:europeanaId, :resourceUrl, :status)")
        .dataSource(dataSource)
        .beanMapped()
        .build();
  }

  @Bean
  public Job tombstoneCleanUpJob(JobRepository jobRepository, Step initial, JobCompletionNotificationListener listener) {
    return new JobBuilder(jobRepository)
        .listener(listener)
        .start(initial)
        .build();
  }

  @Bean
  public Step tombstoneCleanUpStep(JobRepository jobRepository,
      DataSourceTransactionManager transactionManager,
      FlatFileItemReader<TombstoneEntry> reader,
      TombstoneItemProcessor processor,
      JdbcBatchItemWriter<TombstoneEntry> writer) {
    return new StepBuilder(jobRepository)
        .<TombstoneEntry,TombstoneEntry>chunk(appConfiguration.batchChunkSize())
        .transactionManager(transactionManager)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  }
}
