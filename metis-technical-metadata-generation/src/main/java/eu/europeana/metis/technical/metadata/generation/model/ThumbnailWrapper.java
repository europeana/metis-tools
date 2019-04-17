package eu.europeana.metis.technical.metadata.generation.model;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ThumbnailWrapper {

  private String targetName;
  private byte[] thumbnailBytes;

  public String getTargetName() {
    return targetName;
  }

  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  public byte[] getThumbnailBytes() {
    return thumbnailBytes;
  }

  public void setThumbnailBytes(byte[] thumbnailBytes) {
    this.thumbnailBytes = thumbnailBytes;
  }
}
