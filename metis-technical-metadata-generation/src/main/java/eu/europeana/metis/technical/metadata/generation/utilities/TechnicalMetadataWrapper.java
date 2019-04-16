package eu.europeana.metis.technical.metadata.generation.utilities;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import eu.europeana.metis.core.workflow.HasMongoObjectId;
import eu.europeana.metis.json.ObjectIdSerializer;
import eu.europeana.metis.mediaprocessing.model.ResourceMetadata;
import java.util.List;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class TechnicalMetadataWrapper implements HasMongoObjectId {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;

  @Indexed(options = @IndexOptions(unique = true))
  private String resourceUrl;
  private boolean successExtraction;
  private ResourceMetadata resourceMetadata;
  private List<ThumbnailWrapper> thumbnailWrappers;

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public void setId(ObjectId objectId) {
    this.id = objectId;
  }

  public String getResourceUrl() {
    return resourceUrl;
  }

  public void setResourceUrl(String resourceUrl) {
    this.resourceUrl = resourceUrl;
  }

  public boolean isSuccessExtraction() {
    return successExtraction;
  }

  public void setSuccessExtraction(boolean successExtraction) {
    this.successExtraction = successExtraction;
  }

  public ResourceMetadata getResourceMetadata() {
    return resourceMetadata;
  }

  public void setResourceMetadata(
      ResourceMetadata resourceMetadata) {
    this.resourceMetadata = resourceMetadata;
  }

  public List<ThumbnailWrapper> getThumbnailWrappers() {
    return thumbnailWrappers;
  }

  public void setThumbnailWrappers(
      List<ThumbnailWrapper> thumbnailWrappers) {
    this.thumbnailWrappers = thumbnailWrappers;
  }
}
