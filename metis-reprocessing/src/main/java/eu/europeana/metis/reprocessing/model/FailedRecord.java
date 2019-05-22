package eu.europeana.metis.reprocessing.model;

import org.mongodb.morphia.annotations.Id;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-20
 */
public class FailedRecord {

  @Id
  private String failedUrl;
  private String errorStackTrace;

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
}
