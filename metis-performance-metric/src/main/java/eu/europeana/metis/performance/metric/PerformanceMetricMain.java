package eu.europeana.metis.performance.metric;

import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.performance.metric.model.MetricNumberOfDatasetsPublished;
import eu.europeana.metis.performance.metric.model.MetricNumberOfRecordPerOperations;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Main script to run the performance metrics
 */
public class PerformanceMetricMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMetricMain.class);
    private static final String CONFIGURATION_FILE = "application.properties";
    private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

    private static final DateTimeFormatter OUTPUT_FILE_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss");
    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalDateTime NOW_DATE = LocalDateTime.now();
    private static final String FILE_PATH = "metis-performance-metric/src/main/java/eu/europeana/metis/performance/metric/output/";
    private static final String PERFORMANCE_METRIC_1 = "performance-metric-1-";
    private static final String PERFORMANCE_METRIC_2 = "performance-metric-2-";

    public static void main(String[] args) throws CustomTruststoreAppender.TrustStoreConfigurationException {
        LOGGER.info("Starting script");

        final MongoMetisCoreDao mongoMetisCoreDao = new MongoMetisCoreDao(propertiesHolder);
        MetricNumberOfRecordPerOperations metric1 = new MetricNumberOfRecordPerOperations(mongoMetisCoreDao);
        MetricNumberOfDatasetsPublished metric2 = new MetricNumberOfDatasetsPublished(mongoMetisCoreDao);

        //Prepare input values
        final String startDateAsString = "2023-01-01 00:00:00";
        final String endDateAsString = "2023-05-01 23:59:59";
        final LocalDateTime startDate = LocalDateTime.parse(startDateAsString, INPUT_FORMATTER);
        final LocalDateTime endDate = LocalDateTime.parse(endDateAsString, INPUT_FORMATTER);

        //Start printing metrics
        LOGGER.info("Running metrics 1 script");
        metric1.processMetric(startDate, endDate);
        metric1.toCsv(FILE_PATH + PERFORMANCE_METRIC_1 + NOW_DATE.format(OUTPUT_FILE_NAME_FORMATTER) + ".csv");
        LOGGER.info("Finished metrics 1 script");

        LOGGER.info("Running metrics 2 script");
        metric2.processMetric(startDate, endDate);
        metric2.toCsv(FILE_PATH + PERFORMANCE_METRIC_2 + NOW_DATE.format(OUTPUT_FILE_NAME_FORMATTER) + ".csv");
        LOGGER.info("Finished metrics 2 script");

        mongoMetisCoreDao.close();
        LOGGER.info("End script");
    }

}
