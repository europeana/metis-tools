package eu.europeana.metis.mongo.analyzer.model;

public class DatasetIdMetadata {
  private String datasetId;
  private String about;
  private AboutState aboutState;

  public DatasetIdMetadata(String datasetId, String about, AboutState aboutState) {
    this.datasetId = datasetId;
    this.about = about;
    this.aboutState = aboutState;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public String getAbout() {
    return about;
  }

  public AboutState getAboutState() {
    return aboutState;
  }
}
