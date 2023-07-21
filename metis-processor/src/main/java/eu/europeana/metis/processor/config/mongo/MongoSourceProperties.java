package eu.europeana.metis.processor.config.mongo;

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
public class MongoSourceProperties {

    @Value("${mongo.source.hosts}")
    private String[] mongoSourceHosts;
    @Value("${mongo.source.port}")
    private int[] mongoSourcePorts;
    @Value("${mongo.source.username}")
    private String mongoSourceUsername;
    @Value("${mongo.source.password}")
    private String mongoSourcePassword;
    @Value("${mongo.source.authentication.db}")
    private String mongoSourceAuthenticationDatabase;
    @Value("${mongo.source.db}")
    private String mongoSourceDatabase;
    @Value("${mongo.source.enable.ssl}")
    private boolean mongoSourceEnableSsl;
    @Value("${mongo.source.connection.pool.size}")
    private int metisSourceConnectionPoolSize;
    @Value("${mongo.source.application.name}")
    private String mongoSourceApplicationName;

    public MongoProperties<DataAccessConfigException> getMongoSourceProperties()
            throws DataAccessConfigException {
        final MongoProperties<DataAccessConfigException> properties =
                new MongoProperties<>(DataAccessConfigException::new);
        properties.setAllProperties(mongoSourceHosts, mongoSourcePorts, mongoSourceAuthenticationDatabase,
                mongoSourceUsername, mongoSourcePassword, mongoSourceEnableSsl, null, mongoSourceApplicationName);
        return properties;
    }

    public String getMongoSourceDatabase() {
        return mongoSourceDatabase;
    }

    public int getMetisSourceConnectionPoolSize() {
        return metisSourceConnectionPoolSize;
    }
}
