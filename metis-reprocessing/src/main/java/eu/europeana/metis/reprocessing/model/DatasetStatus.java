package eu.europeana.metis.reprocessing.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import eu.europeana.metis.core.workflow.HasMongoObjectId;
import eu.europeana.metis.json.ObjectIdSerializer;
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
}
