package eu.europeana.metis.processor.config;

import eu.europeana.metis.processor.ProcessorRunner;
import eu.europeana.metis.processor.config.mongo.MongoCoreProperties;
import eu.europeana.metis.processor.config.mongo.MongoProcessorProperties;
import eu.europeana.metis.processor.config.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.dao.MongoCoreDao;
import eu.europeana.metis.processor.dao.MongoProcessorDao;
import eu.europeana.metis.processor.dao.MongoSourceDao;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.invoke.MethodHandles;

@Configuration
public class ApplicationConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    /**
     * Autowired constructor for Spring Configuration class.
     *
     * @param propertiesHolder the object that holds all boot configuration values
     * @throws CustomTruststoreAppender.TrustStoreConfigurationException if the configuration of the truststore failed
     */
    @Autowired
    public ApplicationConfiguration(ConfigurationProperties propertiesHolder) throws CustomTruststoreAppender.TrustStoreConfigurationException {
        ApplicationConfiguration.initializeApplication(propertiesHolder);
    }

    /**
     * This method performs the initializing tasks for the application.
     *
     * @param propertiesHolder The properties.
     * @throws CustomTruststoreAppender.TrustStoreConfigurationException In case a problem occurred with the truststore.
     */
    static void initializeApplication(ConfigurationProperties propertiesHolder)
            throws CustomTruststoreAppender.TrustStoreConfigurationException {

        // Load the trust store file.
        if (StringUtils.isNotEmpty(propertiesHolder.getTruststorePath()) && StringUtils
                .isNotEmpty(propertiesHolder.getTruststorePassword())) {
            CustomTruststoreAppender
                    .appendCustomTrustoreToDefault(propertiesHolder.getTruststorePath(),
                            propertiesHolder.getTruststorePassword());
            LOGGER.info("Custom truststore appended to default truststore");
        }
    }

    @Bean
    public MongoCoreDao getMongoCoreDao(MongoCoreProperties mongoCoreProperties) throws DataAccessConfigException {
        return new MongoCoreDao(mongoCoreProperties);
    }

    @Bean
    public MongoProcessorDao getMongoProcessorDao(MongoProcessorProperties mongoProcessorProperties) throws DataAccessConfigException {
        return new MongoProcessorDao(mongoProcessorProperties);
    }

    @Bean
    public MongoSourceDao getMongoSourceDao(MongoSourceProperties mongoSourceProperties) throws DataAccessConfigException {
        return new MongoSourceDao(mongoSourceProperties);
    }

    @Bean
    public CommandLineRunner commandLineRunner(MongoProcessorDao mongoProcessorDao, MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao) {
        return new ProcessorRunner(mongoProcessorDao, mongoCoreDao, mongoSourceDao);
    }
}
