package eu.europeana.metis.execution.utilities;

/**
 * Contains information about an average, the average is calculated on the fly, based on the
 * contained information
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class AverageMaintainer {

  private int totalSamples;
  private int totalRecords;
  private float totalSeconds;

  public void addSample(int records, float seconds) {
    this.totalSamples++;
    this.totalRecords += records;
    this.totalSeconds += seconds;
  }

  public void addAverageMaintainer(AverageMaintainer averageMaintainer) {
    this.totalSamples += averageMaintainer.getTotalSamples();
    this.totalRecords += averageMaintainer.getTotalRecords();
    this.totalSeconds += averageMaintainer.getTotalSeconds();
  }

  public float getTotalAverageInSecs() {
    return totalSeconds == 0.0f ? 0.0f : totalRecords / totalSeconds;
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

  @Override
  public String toString() {
    final float totalAverageInSecs = getTotalAverageInSecs();
    return String.format(
        "Total Samples: %s, Total Records: %s, Total Seconds: %s, Total Average Speed: %s r/s, %s r/h, %s r/d",
        totalSamples, totalRecords, totalSeconds, totalAverageInSecs, totalAverageInSecs * 3_600,
        totalAverageInSecs * 86_400);
  }
}
