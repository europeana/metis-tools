package eu.europeana.metis.processor.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;

public class Report {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String HEADER_ROW = "RecordId, Status, Elapsed Time, Width Before, Height Before"
            + " Width After, Height After";

    private final String pathToReport;

    public Report(String pathToReport) {
        this.pathToReport = pathToReport;
        initReport();
    }

    private void initReport() {
        try (PrintWriter printWriter = new PrintWriter(pathToReport)) {
            printWriter.println(HEADER_ROW);
        } catch (FileNotFoundException e) {
            LOGGER.error("File path {} does not exist", pathToReport);
        }
    }

    public void appendRow(ReportRow recordRow) {
        try (PrintWriter printWriter = new PrintWriter(pathToReport)) {
            printWriter.append(recordRow.toString());
            printWriter.flush();
        } catch (FileNotFoundException e) {
            LOGGER.error("File path {} does not exist", pathToReport);
        }
    }
}
