package eu.europeana.metis.migration.results.model;

public class DatasetInfo {

  private String nameInDb;
  private String nameInCsv;
  private String stateInCsv;

  DatasetInfo() {
  }

  public DatasetInfo(String datasetNameInDb, String datasetNameInCsv, String datasetState) {
    this.nameInDb = datasetNameInDb;
    this.nameInCsv = datasetNameInCsv;
    this.stateInCsv = datasetState;
  }

  public String getNameInDb() {
    return nameInDb;
  }

  public String getNameInCsv() {
    return nameInCsv;
  }

  public String getStateInCsv() {
    return stateInCsv;
  }
}
