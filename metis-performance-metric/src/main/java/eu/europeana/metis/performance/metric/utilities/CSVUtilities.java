package eu.europeana.metis.performance.metric.utilities;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Date;
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

    public void writeMetric1IntoCsvFile(String filePath, Date startDate, Date endDate) throws FileNotFoundException {
        final File resultFile = new File(filePath);
        final String firstRow = "Date, Import OAI-PMH, Import HTTP, Validate EDM External, Transformation, Validate EDM Internal, " +
                "Normalization, Enrichment, Media Processing, Index to Preview, Index to Publish";

        try (PrintWriter printWriter = new PrintWriter(resultFile)) {

        }

    }

    public void writeMetric2IntoCsvFile(String filePath, Date startDate, Date endDate) throws FileNotFoundException {
        final File resultFile = new File(filePath);
        final String firstRow = "DatasetId, Date Publish Indexing, Time of last successful harvest in hours, Number of Records Published";

        try (PrintWriter printWriter = new PrintWriter(resultFile)) {
            List<String> contentToPrint = performanceMetricsUtilities.getDataForMetric2(startDate, endDate);
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if(StringUtils.isNotEmpty(content)){
                    printWriter.println(content);
                }
            });
        }

    }

}
