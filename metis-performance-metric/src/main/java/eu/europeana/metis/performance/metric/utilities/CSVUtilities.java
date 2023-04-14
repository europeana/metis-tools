package eu.europeana.metis.performance.metric.utilities;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CSVUtilities {

    private final static DateTimeFormatter CUSTOM_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final LocalDateTime NOW_DATE = LocalDateTime.now();
    private static final String FILE_PATH = "metis-performance-metric/src/main/java/eu/europeana/metis/performance/metric/output/";
    private static final String OUTPUT_FILE_NAME = "performance-metric-";
    private final StringBuilder stringBuilder = new StringBuilder();

    public void writeIntoCsvFile() throws FileNotFoundException {
        stringBuilder.append(FILE_PATH).append(OUTPUT_FILE_NAME).append(NOW_DATE.format(CUSTOM_FORMATTER)).append(".csv");
        File resultFile = new File(stringBuilder.toString());
        String firstRow = "DatasetId, Date of Indexing to Publish, Last Successful Harvest, Number of Records Published";

        try(PrintWriter printWriter = new PrintWriter(resultFile)){
            printWriter.println(firstRow);
        }

    }

    private String convertToCSV(String[] data) {
        return String.join(",", data);
    }

}
