package eu.europeana.metis.performance.metric;

import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.performance.metric.utilities.CSVUtilities;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Main script to run the performance metrics
 */
public class PerformanceMetricMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMetricMain.class);
    private static final String CONFIGURATION_FILE = "application.properties";
    private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final LocalDateTime NOW_DATE = LocalDateTime.now();
    private static final String FILE_PATH = "metis-performance-metric/src/main/java/eu/europeana/metis/performance/metric/output/";
    private static final String OUTPUT_FILE_NAME = "performance-metric-";

    public static void main(String[] args) throws CustomTruststoreAppender.TrustStoreConfigurationException, FileNotFoundException, ParseException {
        LOGGER.info("Starting script");

        final MongoMetisCoreDao mongoMetisCoreDao = new MongoMetisCoreDao(propertiesHolder);
        final CSVUtilities csvUtilities = new CSVUtilities(mongoMetisCoreDao);

        //Prepare input values
        final String startDateAsString = "2022-01-01 00:00:00";
        final String endDateAsString = "2022-12-31 23:59:59";
        final SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date startDate = simpleDateFormatter.parse(startDateAsString);
        final Date endDate = simpleDateFormatter.parse(endDateAsString);

        //Start printing
        csvUtilities.writeIntoCsvFile(FILE_PATH + OUTPUT_FILE_NAME + NOW_DATE.format(ISO_FORMATTER) + ".csv", startDate, endDate);

        mongoMetisCoreDao.close();
        LOGGER.info("End script");
    }

}
