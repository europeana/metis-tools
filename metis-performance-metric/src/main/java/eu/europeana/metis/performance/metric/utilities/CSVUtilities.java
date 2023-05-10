package eu.europeana.metis.performance.metric.utilities;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Class dedicated to writing content into a csv file
 */
public class CSVUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    private final PerformanceMetricsUtilities performanceMetricsUtilities;
    public CSVUtilities(PerformanceMetricsUtilities performanceMetricsUtilities) {
        this.performanceMetricsUtilities = performanceMetricsUtilities;
    }

    public void writeMetric1IntoCsvFile(String filePath, LocalDateTime startDate, LocalDateTime endDate) {
        final File resultFile = new File(filePath);
        final String firstRow = "Date, Import OAI-PMH, Import HTTP, Validate EDM External, Transformation, Validate EDM Internal, " +
                "Normalization, Enrichment, Media Processing, Index to Preview, Index to Publish";

        final List<String> output = performanceMetricsUtilities.getDateForMetric1(startDate, endDate);

        printIntoFile(resultFile, firstRow, output);

    }

    public void writeMetric2IntoCsvFile(String filePath, LocalDateTime startDate, LocalDateTime endDate) {
        final File resultFile = new File(filePath);
        final List<String> contentToPrint = performanceMetricsUtilities.getDataForMetric2(startDate, endDate);
        final String firstRow = "DatasetId, Date of Index to Publish, Time since last successful harvest in hours, Number of Records Published";
        printIntoFile(resultFile, firstRow, contentToPrint);

    }

    private void printIntoFile(File fileToWrite, String firstRow, List<String> contentToPrint){
        try(PrintWriter printWriter = new PrintWriter(fileToWrite)){
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if(StringUtils.isNotEmpty(content)){
                    printWriter.println(content);
                }
            });
        } catch (FileNotFoundException e){
            LOGGER.error("File path {} does not exist", fileToWrite.getPath());
        }
    }

}
