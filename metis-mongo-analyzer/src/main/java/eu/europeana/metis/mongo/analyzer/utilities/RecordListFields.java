package eu.europeana.metis.mongo.analyzer.utilities;

public enum RecordListFields {
  //@formatter:off
  PROXIES("proxies"),
  LICENSES("licenses"),
  SERVICES("services"),
  PLACES("places"),
  AGENTS("agents"),
  TIMESPANS("timespans"),
  CONCEPTS("concepts"),
  AGGREGATIONS("aggregations"),
  PROVIDEDCHOS("providedCHOs");
  //@formatter:off

  private String fieldName;

  RecordListFields(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
}
