package eu.europeana.metis.performance.metric.utilities;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Class dedicated to writing content into a csv file
 */
public class CSVUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    public static void printIntoFile(File fileToWrite, String firstRow, List<String> contentToPrint) {
        try (PrintWriter printWriter = new PrintWriter(fileToWrite)) {
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if (StringUtils.isNotEmpty(content)) {
                    printWriter.println(content);
                }
            });
        } catch (FileNotFoundException e) {
            LOGGER.error("File path {} does not exist", fileToWrite.getPath());
        }
    }

}
