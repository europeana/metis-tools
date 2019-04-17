package eu.europeana.metis.technical.metadata.generation.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import eu.europeana.metis.core.workflow.HasMongoObjectId;
import eu.europeana.metis.json.ObjectIdSerializer;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class FileStatus implements HasMongoObjectId {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;

  @Indexed(options = @IndexOptions(unique = true))
  private String fileName;
  private int lineReached;
  private boolean endOfFileReached;

  public FileStatus() {
  }

  public FileStatus(String fileName, int lineReached) {
    this.fileName = fileName;
    this.lineReached = lineReached;
  }

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public void setId(ObjectId objectId) {
    this.id = objectId;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public int getLineReached() {
    return lineReached;
  }

  public void setLineReached(int lineReached) {
    this.lineReached = lineReached;
  }

  private void incrementLineReached() {
    this.lineReached++;
  }

  public boolean isEndOfFileReached() {
    return endOfFileReached;
  }

  public void setEndOfFileReached(boolean endOfFileReached) {
    this.endOfFileReached = endOfFileReached;
  }
}
