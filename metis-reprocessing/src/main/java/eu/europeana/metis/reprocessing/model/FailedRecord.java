package eu.europeana.metis.reprocessing.model;

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;

/**
 * Model class that contains failed record identifiers.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-20
 */
@Entity
public class FailedRecord {

  @Id
  private String failedUrl;
  private String errorStackTrace;
  private boolean successfullyReprocessed;

  public FailedRecord() {
    //Default constructor
  }

  public FailedRecord(String failedUrl) {
    this.failedUrl = failedUrl;
  }

  public FailedRecord(String failedUrl, String errorStackTrace) {
    this.failedUrl = failedUrl;
    this.errorStackTrace = errorStackTrace;
  }

  public String getFailedUrl() {
    return failedUrl;
  }

  public String getErrorStackTrace() {
    return errorStackTrace;
  }

  public boolean isSuccessfullyReprocessed() {
    return successfullyReprocessed;
  }

  public void setSuccessfullyReprocessed(boolean successfullyReprocessed) {
    this.successfullyReprocessed = successfullyReprocessed;
  }
}
