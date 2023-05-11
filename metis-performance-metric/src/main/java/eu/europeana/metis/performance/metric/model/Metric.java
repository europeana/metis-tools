package eu.europeana.metis.performance.metric.model;

import java.time.LocalDateTime;

public abstract class Metric {

    /**
     * Executes the metric within a given date interval
     * @param startLocalDateTime The start date and time of the date interval
     * @param endLocalDateTime The end date and time of the date interval
     */
    public abstract void processMetric(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime);

    /**
     * Prints the contents gathered
     * @param filePath The path of the file to be printed to
     */
    public abstract void toCsv(String filePath);

}
