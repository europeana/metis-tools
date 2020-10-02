package eu.europeana.metis.mongo.analyzer.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatasetAnalysis {

  private String datasetId;
  private int totalRecordsWithDuplicates = 0;
  private Map<Integer, Integer> duplicatesAndQuantity = new HashMap<>();

  public DatasetAnalysis(String datasetId) {
    this.datasetId = datasetId;
  }

  public void incrementTotalRecordsWithDuplicates() {
    totalRecordsWithDuplicates++;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetAnalysis that = (DatasetAnalysis) o;
    return datasetId.equals(that.datasetId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetId);
  }

  public String getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(String datasetId) {
    this.datasetId = datasetId;
  }

  public int getTotalRecordsWithDuplicates() {
    return totalRecordsWithDuplicates;
  }

  public void setTotalRecordsWithDuplicates(int totalRecordsWithDuplicates) {
    this.totalRecordsWithDuplicates = totalRecordsWithDuplicates;
  }

  public Map<Integer, Integer> getDuplicatesAndQuantity() {
    return duplicatesAndQuantity;
  }

  public void setDuplicatesAndQuantity(Map<Integer, Integer> duplicatesAndQuantity) {
    this.duplicatesAndQuantity = duplicatesAndQuantity;
  }
}
