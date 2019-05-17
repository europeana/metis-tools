package eu.europeana.metis.reprocessing.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import eu.europeana.metis.core.workflow.HasMongoObjectId;
import eu.europeana.metis.json.ObjectIdSerializer;
import java.util.HashSet;
import java.util.Set;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class DatasetStatus implements HasMongoObjectId {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;

  @Indexed(options = @IndexOptions(unique = true))
  private String datasetId;
  private long totalProcessed;
  private long totalRecords;
  private long totalFailedRecords;
  private final Set<String> failedRecordsSet = new HashSet<>();
  private long averageTimeRecordProcessing;
  private long averageTimeRecordIndexing;

  public DatasetStatus() {
  }

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public void setId(ObjectId objectId) {
    this.id = objectId;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(String datasetId) {
    this.datasetId = datasetId;
  }

  public long getTotalProcessed() {
    return totalProcessed;
  }

  public void setTotalProcessed(long totalProcessed) {
    this.totalProcessed = totalProcessed;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getTotalFailedRecords() {
    return totalFailedRecords;
  }

  public void setTotalFailedRecords(long totalFailedRecords) {
    this.totalFailedRecords = totalFailedRecords;
  }

  public Set<String> getFailedRecordsSet() {
    return failedRecordsSet;
  }

  public long getAverageTimeRecordProcessing() {
    return averageTimeRecordProcessing;
  }

  public void setAverageTimeRecordProcessing(long averageTimeRecordProcessing) {
    this.averageTimeRecordProcessing = averageTimeRecordProcessing;
  }

  public long getAverageTimeRecordIndexing() {
    return averageTimeRecordIndexing;
  }

  public void setAverageTimeRecordIndexing(long averageTimeRecordIndexing) {
    this.averageTimeRecordIndexing = averageTimeRecordIndexing;
  }
}
