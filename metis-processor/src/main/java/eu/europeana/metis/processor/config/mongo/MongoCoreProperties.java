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
public class MongoCoreProperties {

    @Value("${mongo.metis.core.hosts}")
    private String[] mongoCoreHosts;
    @Value("${mongo.metis.core.port}")
    private int[] mongoCorePorts;
    @Value("${mongo.metis.core.username}")
    private String mongoCoreUsername;
    @Value("${mongo.metis.core.password}")
    private String mongoCorePassword;
    @Value("${mongo.metis.core.authentication.db}")
    private String mongoCoreAuthenticationDatabase;
    @Value("${mongo.metis.core.db}")
    private String mongoCoreDatabase;
    @Value("${mongo.metis.core.enable.ssl}")
    private boolean mongoCoreEnableSsl;
    @Value("${mongo.metis.core.connection.pool.size}")
    private int metisCoreConnectionPoolSize;
    @Value("${mongo.metis.core.application.name}")
    private String mongoCoreApplicationName;

    public MongoProperties<DataAccessConfigException> getMongoCoreProperties()
            throws DataAccessConfigException {
        final MongoProperties<DataAccessConfigException> properties =
                new MongoProperties<>(DataAccessConfigException::new);
        properties.setAllProperties(mongoCoreHosts, mongoCorePorts, mongoCoreAuthenticationDatabase,
                mongoCoreUsername, mongoCorePassword, mongoCoreEnableSsl, null, mongoCoreApplicationName);
        return properties;
    }

    public String getMongoCoreDatabase() {
        return mongoCoreDatabase;
    }

    public int getMetisCoreConnectionPoolSize() {
        return metisCoreConnectionPoolSize;
    }
}
