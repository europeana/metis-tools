package eu.europeana.metis.tombstone.batch;

import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * The type Job completion notification listener.
 */
@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

  private static final Logger log =  LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final JdbcTemplate jdbcTemplate;

  /**
   * Instantiates a new Job completion notification listener.
   *
   * @param jdbcTemplate the jdbc template
   */
  public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
      log.info("<===== Job finished!  =====>");

      jdbcTemplate
          .query("SELECT europeana_id, resource_url, status FROM entry", new DataClassRowMapper<>(TombstoneEntry.class))
          .forEach(entry -> log.info("Found <{}> in the database.", entry));
    }
  }
}

