package eu.europeana.metis.reprocessing.model;

import org.mongodb.morphia.annotations.Id;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-20
 */
public class FailedRecord {

  @Id
  private String failedUrl;

  public FailedRecord() {
    //Default constructor
  }

  public FailedRecord(String failedUrl) {
    this.failedUrl = failedUrl;
  }

  public String getFailedUrl() {
    return failedUrl;
  }

  public void setFailedUrl(String failedUrl) {
    this.failedUrl = failedUrl;
  }
}
