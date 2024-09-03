package eu.europeana.metis.config;

import eu.europeana.metis.mongo.connection.MongoProperties;
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
public class ConfigurationPropertiesHolder {

    //Mongo Metis Statistics Dashboard
    @Value("${mongo.sd.hosts}")
    private String[] mongoSDHosts;
    @Value("${mongo.sd.port}")
    private int[] mongoSDPorts;
    @Value("${mongo.sd.username}")
    private String mongoSDUsername;
    @Value("${mongo.sd.password}")
    private String mongoSDPassword;
    @Value("${mongo.sd.authentication.db}")
    private String mongoSDAuthenticationDatabase;
    @Value("${mongo.sd.db}")
    private String mongoSDDatabase;
    @Value("${mongo.sd.enable.ssl}")
    private boolean mongoSDEnableSsl;
    @Value("${mongo.sd.application.name}")
    private String mongoSDApplicationName;

    //Custom truststore
    @Value("${truststore.path}")
    private String truststorePath;
    @Value("${truststore.password}")
    private String truststorePassword;

    public String getTruststorePath() {
        return truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }


    public MongoProperties<DataAccessConfigException> getMongoSDProperties()
            throws DataAccessConfigException {
        final MongoProperties<DataAccessConfigException> properties =
                new MongoProperties<>(DataAccessConfigException::new);
        properties.setAllProperties(mongoSDHosts, mongoSDPorts, mongoSDAuthenticationDatabase,
                mongoSDUsername, mongoSDPassword, mongoSDEnableSsl, null, mongoSDApplicationName);
        return properties;
    }

    public String getMongoSDDatabase() {
        return mongoSDDatabase;
    }

}
