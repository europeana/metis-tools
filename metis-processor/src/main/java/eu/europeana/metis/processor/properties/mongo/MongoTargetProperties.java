package eu.europeana.metis.processor.properties.mongo;

import eu.europeana.metis.mongo.connection.MongoProperties;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Class that is used to read all configuration properties for the application.
 * <p>
 * It uses {@link PropertySource} to identify the properties on application startup
 * </p>
 */
@Component
public class MongoTargetProperties {

    @Value("${mongo.target.hosts}")
    private String[] mongoTargetHosts;
    @Value("${mongo.target.port}")
    private int[] mongoTargetPorts;
    @Value("${mongo.target.username}")
    private String mongoTargetUsername;
    @Value("${mongo.target.password}")
    private String mongoTargetPassword;
    @Value("${mongo.target.authentication.db}")
    private String mongoTargetAuthenticationDatabase;
    @Value("${mongo.target.db}")
    private String mongoTargetDatabase;
    @Value("${mongo.target.enable.ssl}")
    private boolean mongoTargetEnableSsl;
    @Value("${mongo.target.connection.pool.size}")
    private int metisTargetConnectionPoolSize;
    @Value("${mongo.target.application.name}")
    private String mongoTargetApplicationName;

    public MongoProperties<DataAccessConfigException> getMongoTargetProperties()
            throws DataAccessConfigException {
        final MongoProperties<DataAccessConfigException> properties =
                new MongoProperties<>(DataAccessConfigException::new);
        properties.setAllProperties(mongoTargetHosts, mongoTargetPorts, mongoTargetAuthenticationDatabase,
                mongoTargetUsername, mongoTargetPassword, mongoTargetEnableSsl, null, mongoTargetApplicationName);
        return properties;
    }

    public String[] getMongoTargetHosts() {
        return mongoTargetHosts;
    }

    public int[] getMongoTargetPorts() {
        return mongoTargetPorts;
    }

    public String getMongoTargetUsername() {
        return mongoTargetUsername;
    }

    public String getMongoTargetPassword() {
        return mongoTargetPassword;
    }

    public String getMongoTargetAuthenticationDatabase() {
        return mongoTargetAuthenticationDatabase;
    }

    public boolean isMongoTargetEnableSsl() {
        return mongoTargetEnableSsl;
    }

    public String getMongoTargetApplicationName() {
        return mongoTargetApplicationName;
    }

    public String getMongoTargetDatabase() {
        return mongoTargetDatabase;
    }

    public int getMetisTargetConnectionPoolSize() {
        return metisTargetConnectionPoolSize;
    }
}
