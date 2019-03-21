package eu.europeana.metis.execution.utilities;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class AverageMaintainer {

  private int totalSamples;
  private int totalRecords;
  private float totalSeconds;
  private float totalAverageInSecs;

  public void addSample(int records, float seconds) {
    this.totalSamples++;
    this.totalRecords += records;
    this.totalSeconds += seconds;
    totalAverageInSecs = totalRecords / totalSeconds;
  }

  public float getTotalAverageInSecs() {
    return totalAverageInSecs;
  }

  public int getTotalSamples() {
    return totalSamples;
  }

  public int getTotalRecords() {
    return totalRecords;
  }

  public float getTotalSeconds() {
    return totalSeconds;
  }

  public void reset() {
    totalRecords = 0;
    totalSeconds = 0;
  }
}
