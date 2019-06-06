package eu.europeana.metis.technical.metadata.generation.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import eu.europeana.metis.json.ObjectIdSerializer;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;

/**
 * Model class that contains the status of the thumbnail upload process for a specific file.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-05
 */
public class ThumbnailFileStatus implements DatasetFileStatus {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;

  @Indexed(options = @IndexOptions(unique = true))
  private String fileName;
  private int lineReached;
  private boolean endOfFileReached;

  public ThumbnailFileStatus() {
  }

  public ThumbnailFileStatus(String fileName, int lineReached) {
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

  @Override
  public String getFileName() {
    return fileName;
  }

  @Override
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public int getLineReached() {
    return lineReached;
  }

  @Override
  public void setLineReached(int lineReached) {
    this.lineReached = lineReached;
  }

  @Override
  public boolean isEndOfFileReached() {
    return endOfFileReached;
  }

  @Override
  public void setEndOfFileReached(boolean endOfFileReached) {
    this.endOfFileReached = endOfFileReached;
  }

  @Override
  public String toString() {
    return String
        .format("ObjectId: %s, fileName: %s, lineReached: %d, endOfFileReached: %b", id, fileName,
            lineReached, endOfFileReached);
  }
}
