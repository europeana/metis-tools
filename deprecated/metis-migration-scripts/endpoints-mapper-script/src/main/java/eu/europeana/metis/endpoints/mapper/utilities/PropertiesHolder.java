package eu.europeana.metis.endpoints.mapper.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class PropertiesHolder {

  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");

  public final String organizationId;
  public final String europeanaOaiEndpoint;
  public final Mode mode;
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnablessl;
  public final String mongoDbOriginal;
  public final String mongoDbTemporary;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(Thread.currentThread().getContextClassLoader()
          .getResource(configurationFileName).getFile()));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
    organizationId = properties.getProperty("organization.id");
    europeanaOaiEndpoint = properties.getProperty("europeana.oai.endpoint");
    mode = Mode.getModeFromEnumName(properties.getProperty("mode"));
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
    mongoHosts = properties.getProperty("mongo.hosts").split(",");
    mongoPorts = Arrays.stream(properties.getProperty("mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    mongoAuthenticationDb = properties.getProperty("mongo.authentication.db");
    mongoUsername = properties.getProperty("mongo.username");
    mongoPassword = properties.getProperty("mongo.password");
    mongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.enableSSL"));
    mongoDbOriginal = properties.getProperty("mongo.db.original");
    mongoDbTemporary = properties.getProperty("mongo.db.temporary");
  }
}
