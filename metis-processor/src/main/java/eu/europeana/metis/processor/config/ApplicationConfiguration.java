package eu.europeana.metis.processor.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerClient;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerScript;
import eu.europeana.metis.image.enhancement.config.ImageEnhancerClientConfig;
import eu.europeana.metis.image.enhancement.domain.worker.ImageEnhancerWorker;
import eu.europeana.metis.processor.ProcessorRunner;
import eu.europeana.metis.processor.dao.MongoCoreDao;
import eu.europeana.metis.processor.dao.MongoProcessorDao;
import eu.europeana.metis.processor.dao.MongoSourceDao;
import eu.europeana.metis.processor.dao.MongoTargetDao;
import eu.europeana.metis.processor.properties.general.ApplicationProperties;
import eu.europeana.metis.processor.properties.general.ImageEnhancerClientProperties;
import eu.europeana.metis.processor.properties.general.RedisProperties;
import eu.europeana.metis.processor.properties.general.S3Properties;
import eu.europeana.metis.processor.properties.general.SolrZookeeperTargetProperties;
import eu.europeana.metis.processor.properties.general.TruststoreProperties;
import eu.europeana.metis.processor.properties.mongo.MongoCoreProperties;
import eu.europeana.metis.processor.properties.mongo.MongoProcessorProperties;
import eu.europeana.metis.processor.properties.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.properties.mongo.MongoTargetProperties;
import eu.europeana.metis.processor.utilities.ImageEnhancerUtil;
import eu.europeana.metis.processor.utilities.FileCsvImageReporter;
import eu.europeana.metis.processor.utilities.S3Client;
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

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@Configuration
public class ApplicationConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private FileCsvImageReporter fileCsvImageReporter;

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
    public RedissonClient getRedissonClient(RedisProperties redisProperties, TruststoreProperties truststoreProperties) throws MalformedURLException {
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

    @Bean
    public AmazonS3 getAmazonS3(S3Properties s3Properties) {

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3Properties.getS3AccessKey(), s3Properties.getS3SecretKey()));
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(s3Properties.getS3Endpoint(), Regions.DEFAULT_REGION.getName());

        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @Bean
    public S3Client getS3Client(AmazonS3 amazonS3, S3Properties s3Properties) {
        return new S3Client(amazonS3, s3Properties.getS3BucketName());
    }

    @Bean
    public ImageEnhancerClient getImageEnhancerClient(ImageEnhancerClientProperties imageEnhancerClientProperties) {
        ImageEnhancerClientConfig enhancerClientConfig = new ImageEnhancerClientConfig(imageEnhancerClientProperties.getImageEnhancerEndpoint(), imageEnhancerClientProperties.getImageEnhancerConnectTimeout(), imageEnhancerClientProperties.getImageEnhancerReadTimeout());
        return new ImageEnhancerClient(enhancerClientConfig);
    }

    @Bean
    public ImageEnhancerUtil getImageEnhancerUtil(S3Client s3Client, ImageEnhancerClientProperties imageEnhancerClientProperties) throws IOException {
        fileCsvImageReporter = new FileCsvImageReporter();
        ImageEnhancerWorker imageEnhancerWorker = new ImageEnhancerWorker(
                new ImageEnhancerScript(imageEnhancerClientProperties.getImageEnhancerScriptPath()));
        return new ImageEnhancerUtil(s3Client, imageEnhancerWorker, fileCsvImageReporter);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationProperties applicationProperties, MongoProcessorDao mongoProcessorDao,
                                               MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao,
                                               RedissonClient redissonClient, IndexerPool indexerPool, ImageEnhancerUtil imageEnhancerUtil) {
        return new ProcessorRunner(applicationProperties, mongoProcessorDao, mongoCoreDao, mongoSourceDao, redissonClient, indexerPool, imageEnhancerUtil);
    }
    /**
     * Close resources
     */
    @PreDestroy
    public void close() {
        if (fileCsvImageReporter != null) {
            fileCsvImageReporter.close();
        }
    }

}
