package eu.europeana.metis.migration.counter.domain.service;

import com.mongodb.MongoClient;
import eu.europeana.metis.migration.counter.config.MongoInitializer;
import eu.europeana.metis.migration.counter.dao.MetisCoreMongoDao;
import java.time.Instant;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameter;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MainBatchJobRunnerService implements CommandLineRunner, ExitCodeGenerator {

  private final TaskExecutor pipelineTaskExecutor;
  private final JobOperator jobOperator;
  private final Job job;
  private int exitCode = 0;
  private final MongoInitializer mongoInitializer;
  public MainBatchJobRunnerService(@Qualifier("pipelineTaskExecutor") TaskExecutor pipelineTaskExecutor,
      JobOperator jobOperator,
      Job job,
      MongoInitializer mongoInitializer) {
    this.pipelineTaskExecutor = pipelineTaskExecutor;
    this.jobOperator = jobOperator;
    this.job = job;
    this.mongoInitializer = mongoInitializer;
  }

  @Override
  public void run(String[] args) throws Exception {
    try {
      pipelineTaskExecutor.execute(() -> {
        try {
          JobParameter<String> jobParameter = new JobParameter<>("executionId", "executionId" + Instant.now().toEpochMilli(),
              String.class);
          JobParameters jobParameters = new JobParameters(Set.of(jobParameter));

          JobExecution execution = jobOperator.start(job, jobParameters);
          log.info("Job execution with id {}::{}", execution.getId(), execution.getStatus());
          if (execution.getStatus().isUnsuccessful()) {
            this.exitCode = 1; // Set non-zero code for failures
          }
        } catch (JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException | InvalidJobParametersException |
                 JobRestartException e) {
          log.error("Job execution failed", e);
          this.exitCode = 1;
        }
      });
    } catch (Exception e) {
      log.error("Exception while running BatchJobRunner", e);
      this.exitCode = 1;
      throw e;
    }
    mongoInitializer.close();
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

}
