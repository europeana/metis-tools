package eu.europeana.metis.performance.metric;

import eu.europeana.metis.performance.metric.config.PropertiesHolder;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceMetricMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMetricMain.class);
    private static final String CONFIGURATION_FILE = "application.properties";
    private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

    public static void main(String[] args) throws CustomTruststoreAppender.TrustStoreConfigurationException {
        System.out.println("Hello world!");

        final MongoMetisCoreDao mongoMetisCoreDao = new MongoMetisCoreDao(propertiesHolder);


    }
}
