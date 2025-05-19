package eu.europeana.metis.depublishing.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 */
public class PropertiesHolder {

  public static final Marker STATISTICS_LOGS_MARKER = MarkerFactory.getMarker("STATISTICS_LOGS");
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);
  //General parameters
  public final int minParallelDatasets;
  public final int maxParallelThreadsPerDataset;
  public final int startFromDatasetIndex;
  public final int endAtDatasetIndex;
  public final int mongoPageSize;
  public final Mode mode;
  public final List<String> datasetIdsToProcess;
  public final List<String> recordIdsToProcess;
  public final boolean identityProcess;
  public final boolean depublicationEnabled;

  //Metis Core Mongo
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] metisCoreMongoHosts;
  public final int[] metisCoreMongoPorts;
  public final String metisCoreMongoAuthenticationDb;
  public final String metisCoreMongoUsername;
  public final String metisCoreMongoPassword;
  public final boolean metisCoreMongoEnableSSL;
  public final String metisCoreMongoDb;
  public final int metisCoreConnectionPoolSize;

  //Mongo Destination
  public final String[] mongoHosts;
  public final int[] mongoPorts;
  public final String mongoAuthenticationDb;
  public final String mongoUsername;
  public final String mongoPassword;
  public final boolean mongoEnableSSL;
  public final String mongoDb;
  public final int mongoConnectionPoolSize;
  public final String mongoTombstoneDb;

  //Solr/Zookeeper Destination
  public final String[] destinationSolrHosts;
  public final String[] destinationZookeeperHosts;
  public final int[] destinationZookeeperPorts;
  public final String destinationZookeeperChroot;
  public final String destinationZookeeperDefaultCollection;

  public final Properties properties = new Properties();

  public PropertiesHolder(String configurationFileName) {
    final URL resource = getClass().getClassLoader().getResource(configurationFileName);
    final String filePathInResources = resource == null ? null : resource.getFile();
    String filePath;
    if (filePathInResources != null && new File(filePathInResources).exists()) {
      LOGGER.info("Will try to load {} properties file", filePathInResources);
      filePath = filePathInResources;
    } else {
      LOGGER.info(
          "{} properties file does NOT exist, probably running in standalone .jar mode where the properties file should be on the same directory "
              + "as the .jar file is. Will try to load {} properties file", filePathInResources,
          configurationFileName);
      filePath = configurationFileName;
    }
    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      properties.load(fileInputStream);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }

    //General parameters
    minParallelDatasets = Integer.parseInt(properties.getProperty("min.parallel.datasets"));
    maxParallelThreadsPerDataset = Integer
        .parseInt(properties.getProperty("max.parallel.threads.per.dataset"));
    startFromDatasetIndex =
        StringUtils.isBlank(properties.getProperty("start.from.dataset.index")) ? 0
            : Integer.parseInt(properties.getProperty("start.from.dataset.index"));
    endAtDatasetIndex =
        StringUtils.isBlank(properties.getProperty("end.at.dataset.index")) ? Integer.MAX_VALUE
            : Integer.parseInt(properties.getProperty("end.at.dataset.index"));
    mongoPageSize = Integer.parseInt(properties.getProperty("mongo.page.size"));
    mode = Mode.getModeFromEnumName(properties.getProperty("mode"));

    datasetIdsToProcess = Arrays.stream(properties.getProperty("dataset.ids.to.process").split(","))
                                .filter(StringUtils::isNotBlank).map(String::trim).toList();
    recordIdsToProcess = Arrays.stream(properties.getProperty("record.ids.to.process").split(","))
                               .filter(StringUtils::isNotBlank).map(String::trim).toList();
    identityProcess = Boolean.parseBoolean(properties.getProperty("identity.process"));
    depublicationEnabled = Boolean.parseBoolean(properties.getProperty("depublication.enabled"));

    //Metis Core Mongo
    truststorePath = properties.getProperty("truststore.path");
    truststorePassword = properties.getProperty("truststore.password");
    metisCoreMongoHosts = properties.getProperty("mongo.metis.core.hosts").split(",");

    if (StringUtils.isBlank(properties.getProperty("mongo.metis.core.port"))) {
      metisCoreMongoPorts = null;
    } else {
      metisCoreMongoPorts = Arrays
          .stream(properties.getProperty("mongo.metis.core.port").split(","))
          .mapToInt(Integer::parseInt).toArray();
    }
    metisCoreMongoAuthenticationDb = properties.getProperty("mongo.metis.core.authentication.db");
    metisCoreMongoUsername = properties.getProperty("mongo.metis.core.username");
    metisCoreMongoPassword = properties.getProperty("mongo.metis.core.password");
    metisCoreMongoEnableSSL = Boolean
        .parseBoolean(properties.getProperty("mongo.metis.core.enableSSL"));
    metisCoreMongoDb = properties.getProperty("mongo.metis.core.db");
    metisCoreConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.metis.core.connection.pool.size"), 50);

    //Mongo Destination
    mongoHosts = properties.getProperty("mongo.destination.hosts").split(",");
    mongoPorts = Arrays
        .stream(properties.getProperty("mongo.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    mongoAuthenticationDb = properties
        .getProperty("mongo.destination.authentication.db");
    mongoUsername = properties.getProperty("mongo.destination.username");
    mongoPassword = properties.getProperty("mongo.destination.password");
    mongoEnableSSL = Boolean
        .parseBoolean(properties.getProperty("mongo.destination.enableSSL"));
    mongoDb = properties.getProperty("mongo.destination.db");
    mongoConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.destination.connection.pool.size"), 500);
    mongoTombstoneDb = properties.getProperty("mongo.destination.tombstone.db");

    //Solr/Zookeeper Destination
    destinationSolrHosts = properties.getProperty("solr.destination.hosts").split(",");
    destinationZookeeperHosts = properties.getProperty("zookeeper.destination.hosts").split(",");
    destinationZookeeperPorts = Arrays
        .stream(properties.getProperty("zookeeper.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationZookeeperChroot = properties.getProperty("zookeeper.destination.chroot");
    destinationZookeeperDefaultCollection = properties
        .getProperty("zookeeper.destination.defaultCollection");
  }
}
