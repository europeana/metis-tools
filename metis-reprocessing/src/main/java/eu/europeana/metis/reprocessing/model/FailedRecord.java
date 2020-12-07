package eu.europeana.metis.reprocessing.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Field;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Index;
import dev.morphia.annotations.IndexOptions;
import dev.morphia.annotations.Indexes;
import eu.europeana.metis.mongo.model.HasMongoObjectId;
import eu.europeana.metis.mongo.utils.ObjectIdSerializer;
import org.bson.types.ObjectId;

/**
 * Model class that contains failed record identifiers.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-20
 */
@Entity("FailedRecord")
@Indexes({@Index(fields = {@Field("failedUrl")}, options = @IndexOptions(unique = true))})
public class FailedRecord implements HasMongoObjectId {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;
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

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public void setId(ObjectId objectId) {
    this.id = objectId;
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
