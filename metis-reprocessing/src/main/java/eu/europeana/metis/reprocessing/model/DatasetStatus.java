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
  private long totalRecords;
  private long totalProcessed;
  private long totalFailedRecords;
  private final Set<String> failedRecordsSet = new HashSet<>();
  private long totalTimeProcessing;
  private long totalTimeIndexing;
  private long averageTimeRecordProcessing;
  private long averageTimeRecordIndexing;

  public DatasetStatus() {
    //Default constructor
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

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getTotalProcessed() {
    return totalProcessed;
  }

  public void setTotalProcessed(long totalProcessed) {
    this.totalProcessed = totalProcessed;
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

  public long getTotalTimeProcessing() {
    return totalTimeProcessing;
  }

  public void setTotalTimeProcessing(long totalTimeProcessing) {
    this.totalTimeProcessing = totalTimeProcessing;
  }

  public long getTotalTimeIndexing() {
    return totalTimeIndexing;
  }

  public void setTotalTimeIndexing(long totalTimeIndexing) {
    this.totalTimeIndexing = totalTimeIndexing;
  }

  private double nanoTimeToSeconds(long nanoTime) {
    return (double) nanoTime / 1_000_000_000.0;
  }

  private double secondsTimeToHours(double secondTime) {
    return secondTime / 3600;
  }

  @Override
  public String toString() {
    final double totalTimeProcessingInSeconds = nanoTimeToSeconds(totalTimeProcessing);
    final double totalTimeIndexingInSeconds = nanoTimeToSeconds(totalTimeIndexing);
    return String.format(
        "ObjectId: %s, datasetId: %s, totalRecords: %d, totalProcessed: %d, totalFailedRecords: %d, "
            + "failedRecordsSetSize: %d, totalTimeProcessing: %fs = %fh, totalTimeIndexing: %fs = %fh, "
            + "averageTimeRecordProcessing: %fs, averageTimeRecordIndexing: %fs",
        id, datasetId, totalRecords, totalProcessed, totalFailedRecords, failedRecordsSet.size(),
        totalTimeProcessingInSeconds, secondsTimeToHours(totalTimeProcessingInSeconds),
        totalTimeIndexingInSeconds, secondsTimeToHours(totalTimeIndexingInSeconds),
        nanoTimeToSeconds(averageTimeRecordProcessing),
        nanoTimeToSeconds(averageTimeRecordIndexing));
  }
}
