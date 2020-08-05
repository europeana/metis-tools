package eu.europeana.metis.creator;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dev.morphia.annotations.Id;
import eu.europeana.enrichment.api.external.ObjectIdSerializer;
import org.bson.types.ObjectId;

/**
 * @author Simon Tzanakis
 * @since 2020-08-04
 */
public class LabelInfo {

  @Id
  @JsonSerialize(using = ObjectIdSerializer.class)
  private ObjectId id;
  private String lang;
  private String originalLabel;
  private String lowerCaseLabel;

  public LabelInfo() {
  }

  public LabelInfo(String originalLabel, String lowerCaseLabel, String lang) {
    this.originalLabel = originalLabel;
    this.lowerCaseLabel = lowerCaseLabel;
    this.lang = lang;
  }

  public ObjectId getId() {
    return id;
  }

  public void setId(ObjectId id) {
    this.id = id;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public String getOriginalLabel() {
    return originalLabel;
  }

  public void setOriginalLabel(String originalLabel) {
    this.originalLabel = originalLabel;
  }

  public String getLowerCaseLabel() {
    return lowerCaseLabel;
  }

  public void setLowerCaseLabel(String lowerCaseLabel) {
    this.lowerCaseLabel = lowerCaseLabel;
  }
}
