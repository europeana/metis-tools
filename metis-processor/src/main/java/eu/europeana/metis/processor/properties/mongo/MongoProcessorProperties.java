package eu.europeana.metis.processor.properties.mongo;

import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Class that is used to read all configuration properties for the application.
 * <p>
 * It uses {@link PropertySource} to identify the properties on application startup
 * </p>
 */
@Component
public class MongoProcessorProperties {

    @Value("${mongo.metis.processor.hosts}")
    private String[] mongoProcessorHosts;
    @Value("${mongo.metis.processor.port}")
    private int[] mongoProcessorPorts;
    @Value("${mongo.metis.processor.username}")
    private String mongoProcessorUsername;
    @Value("${mongo.metis.processor.password}")
    private String mongoProcessorPassword;
    @Value("${mongo.metis.processor.authentication.db}")
    private String mongoProcessorAuthenticationDatabase;
    @Value("${mongo.metis.processor.db}")
    private String mongoProcessorDatabase;
    @Value("${mongo.metis.processor.enable.ssl}")
    private boolean mongoProcessorEnableSsl;
    @Value("${mongo.metis.processor.connection.pool.size}")
    private int metisProcessorConnectionPoolSize;
    @Value("${mongo.metis.processor.application.name}")
    private String mongoProcessorApplicationName;

    public MongoProperties<DataAccessConfigException> getMongoProcessorProperties()
            throws DataAccessConfigException {
        final MongoProperties<DataAccessConfigException> properties =
                new MongoProperties<>(DataAccessConfigException::new);
        properties.setAllProperties(mongoProcessorHosts, mongoProcessorPorts, mongoProcessorAuthenticationDatabase,
                mongoProcessorUsername, mongoProcessorPassword, mongoProcessorEnableSsl, null, mongoProcessorApplicationName);
        return properties;
    }

    public String getMongoProcessorDatabase() {
        return mongoProcessorDatabase;
    }

    public int getMetisProcessorConnectionPoolSize() {
        return metisProcessorConnectionPoolSize;
    }
}
