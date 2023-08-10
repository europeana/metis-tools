package eu.europeana.metis.processor.utilities;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileCsvImageReporter {
    private static final String HEADER_ROW =
            "RecordId,imageLink,thumbnailObjectHash,Status,Elapsed Time(ms),Width Before,Height Before,Width After,Height After";
    private final PrintWriter printWriter;

    public FileCsvImageReporter() throws IOException {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        File file = new File("report_" + dateFormat.format(date) + ".csv");
        printWriter = new PrintWriter(new FileWriter(file));
        printWriter.println(HEADER_ROW);
    }

    public void appendRow(ReportRow recordRow) {
        synchronized (this) {
            printWriter.println(recordRow.toString());
            printWriter.flush();
        }
    }

    public void close() {
        printWriter.flush();
        printWriter.close();
    }
}
