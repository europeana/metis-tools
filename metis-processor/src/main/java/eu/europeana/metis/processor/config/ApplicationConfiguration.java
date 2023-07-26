package eu.europeana.metis.processor.config;

import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.processor.ProcessorRunner;
import eu.europeana.metis.processor.config.general.*;
import eu.europeana.metis.processor.config.mongo.MongoCoreProperties;
import eu.europeana.metis.processor.config.mongo.MongoProcessorProperties;
import eu.europeana.metis.processor.config.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.config.mongo.MongoTargetProperties;
import eu.europeana.metis.processor.dao.MongoCoreDao;
import eu.europeana.metis.processor.dao.MongoProcessorDao;
import eu.europeana.metis.processor.dao.MongoSourceDao;
import eu.europeana.metis.processor.dao.MongoTargetDao;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

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
    public ApplicationConfiguration(TruststoreProperties propertiesHolder) throws CustomTruststoreAppender.TrustStoreConfigurationException {
        ApplicationConfiguration.initializeApplication(propertiesHolder);
    }

    /**
     * This method performs the initializing tasks for the application.
     *
     * @param propertiesHolder The properties.
     * @throws CustomTruststoreAppender.TrustStoreConfigurationException In case a problem occurred with the truststore.
     */
    static void initializeApplication(TruststoreProperties propertiesHolder)
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
    public MongoTargetDao getMongoTargetDao(MongoTargetProperties mongoTargetProperties) throws DataAccessConfigException {
        return new MongoTargetDao(mongoTargetProperties);
    }

    @Bean
    public IndexingSettingsProvider getIndexingSettingsProvider(MongoTargetProperties mongoTargetProperties, SolrZookeeperTargetProperties solrZookeeperTargetProperties) {
        return new IndexingSettingsProvider(mongoTargetProperties, solrZookeeperTargetProperties);
    }

    @Bean
    public IndexerPool getIndexerPool(IndexingSettingsProvider indexingSettingsProvider) throws IndexingException, URISyntaxException {
        IndexerFactory indexerFactory = new IndexerFactory(indexingSettingsProvider.getIndexingSettings());
        return new IndexerPool(indexerFactory, 600, 60);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationProperties applicationProperties, MongoProcessorDao mongoProcessorDao, MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao,
                                               RedissonClient redissonClient, IndexerPool indexerPool) {
        return new ProcessorRunner(applicationProperties, mongoProcessorDao, mongoCoreDao, mongoSourceDao, redissonClient, indexerPool);
    }

    @Bean
    RedissonClient getRedissonClient(RedisProperties redisProperties, TruststoreProperties truststoreProperties) throws MalformedURLException {
        Config config = new Config();

        SingleServerConfig singleServerConfig;
        if (redisProperties.isRedisEnableSSL()) {
            singleServerConfig = config.useSingleServer().setAddress(String
                    .format("rediss://%s:%s", redisProperties.getRedisHost(),
                            redisProperties.getRedisPort()));
            LOGGER.info("Redis enabled SSL");
            if (redisProperties.isRedisEnableCustomTruststore()) {
                singleServerConfig
                        .setSslTruststore(Paths.get(truststoreProperties.getTruststorePath()).toUri().toURL());
                singleServerConfig.setSslTruststorePassword(truststoreProperties.getTruststorePassword());
                LOGGER.info("Redis enabled SSL using custom Truststore");
            }
        } else {
            singleServerConfig = config.useSingleServer().setAddress(String
                    .format("redis://%s:%s", redisProperties.getRedisHost(),
                            redisProperties.getRedisPort()));
            LOGGER.info("Redis disabled SSL");
        }
        if (StringUtils.isNotEmpty(redisProperties.getRedisUsername())) {
            singleServerConfig.setUsername(redisProperties.getRedisUsername());
        }
        if (StringUtils.isNotEmpty(redisProperties.getRedisPassword())) {
            singleServerConfig.setPassword(redisProperties.getRedisPassword());
        }

        singleServerConfig.setConnectionPoolSize(redisProperties.getRedissonConnectionPoolSize())
                .setConnectionMinimumIdleSize(redisProperties.getRedissonConnectionPoolSize())
                .setConnectTimeout(redisProperties.getRedissonConnectTimeoutInMillisecs())
                .setDnsMonitoringInterval(redisProperties.getRedissonDnsMonitorIntervalInMillisecs())
                .setIdleConnectionTimeout(redisProperties.getRedissonIdleConnectionTimeoutInMillisecs())
                .setRetryAttempts(redisProperties.getRedissonRetryAttempts());
        config.setLockWatchdogTimeout(TimeUnit.SECONDS.toMillis(redisProperties
                .getRedissonLockWatchdogTimeoutInSecs())); //Give some secs to unlock if connection lost, or if too long to unlock
        return Redisson.create(config);
    }
}
