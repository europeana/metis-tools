package eu.europeana.metis.technical.metadata.generation.model;

import org.mongodb.morphia.annotations.Id;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class FileStatus {

  @Id
  private String filePath;
  private int lineReached;
  private boolean endOfFileReached;

  public FileStatus() {
  }

  public FileStatus(String filePath, int lineReached) {
    this.filePath = filePath;
    this.lineReached = lineReached;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
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
