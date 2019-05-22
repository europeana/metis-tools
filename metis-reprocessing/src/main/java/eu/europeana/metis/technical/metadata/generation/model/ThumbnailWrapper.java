package eu.europeana.metis.technical.metadata.generation.model;

/**
 * Model class that contains the thumbnail bytes the target name of the thumbnail, that needs to
 * identify the thumbnail (e.g. in S3).
 *
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
