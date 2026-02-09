package eu.europeana.metis.tombstone;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@SpringBatchTest
public class TombstoneBatchTest {

  @Autowired
  private JobOperatorTestUtils jobOperatorTestUtils;

  @Test
  public void should_process_all_records_successfully_when_no_error(@Autowired Job job) throws Exception {
    jobOperatorTestUtils.setJob(job);
    var jobExecution = jobOperatorTestUtils.startJob();
    assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
  }

}
