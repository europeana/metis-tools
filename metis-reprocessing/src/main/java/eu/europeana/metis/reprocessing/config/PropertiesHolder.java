package eu.europeana.metis.reprocessing.config;

import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);
  public static final Marker STATISTICS_LOGS_MARKER = MarkerFactory.getMarker("STATISTICS_LOGS");

  //General parameters
  public final int minParallelDatasets;
  public final int maxParallelThreadsPerDataset;
  public final int startFromDatasetIndex;
  public final int endAtDatasetIndex;
  public final int sourceMongoPageSize;
  public final Mode mode;
  public final List<String> datasetIdsToProcess;
  public final boolean identityProcess;
  public final boolean cleanDatabasesBeforeProcess;
  public final boolean tierRecalculation;

  public final ExecutablePluginType reprocessBasedOnPluginType;
  public final List<ExecutablePluginType> invalidatePluginTypes;

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
  //Mongo Source
  public final String[] sourceMongoHosts;
  public final int[] sourceMongoPorts;
  public final String sourceMongoAuthenticationDb;
  public final String sourceMongoUsername;
  public final String sourceMongoPassword;
  public final boolean sourceMongoEnableSSL;
  public final String sourceMongoDb;
  public final int sourceMongoConnectionPoolSize;
  // TODO: 15/05/2023 Temporary field for translations
  public final String sourceTranslationsMongoDb;
  //Mongo Destination
  public final String[] destinationMongoHosts;
  public final int[] destinationMongoPorts;
  public final String destinationMongoAuthenticationDb;
  public final String destinationMongoUsername;
  public final String destinationMongoPassword;
  public final boolean destinationMongoEnableSSL;
  public final String destinationMongoDb;
  public final int destinationMongoConnectionPoolSize;
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
    try {
      properties.load(new FileInputStream(filePath));
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
    sourceMongoPageSize = Integer.parseInt(properties.getProperty("source.mongo.page.size"));
    mode = Mode.getModeFromEnumName(properties.getProperty("mode"));

    datasetIdsToProcess = Arrays.stream(properties.getProperty("dataset.ids.to.process").split(","))
                                .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());
    identityProcess = Boolean.parseBoolean(properties.getProperty("identity.process"));
    cleanDatabasesBeforeProcess = Boolean
        .parseBoolean(properties.getProperty("clean.databases.before.process"));
    tierRecalculation = Boolean.parseBoolean(properties.getProperty("tier.recalculation"));
    reprocessBasedOnPluginType = ExecutablePluginType
        .getPluginTypeFromEnumName(properties.getProperty("reprocess.based.on.plugin.type"));
    invalidatePluginTypes = Arrays
        .stream(properties.getProperty("invalidate.plugin.types").split(","))
        .filter(StringUtils::isNotBlank).map(String::trim)
        .map(ExecutablePluginType::getPluginTypeFromEnumName).collect(Collectors.toList());

    if (mode.equals(Mode.POST_PROCESS) && (reprocessBasedOnPluginType == null || CollectionUtils
        .isEmpty(invalidatePluginTypes))) {
      throw new IllegalArgumentException(String.format("If mode is: %s, the "
          + "reprocessBasedOnPluginType must not be null and invalidatePluginTypes must not be "
          + "empty", Mode.POST_PROCESS));
    }

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

    //Mongo Source
    sourceMongoHosts = properties.getProperty("mongo.source.hosts").split(",");
    sourceMongoPorts = Arrays.stream(properties.getProperty("mongo.source.port").split(","))
                             .mapToInt(Integer::parseInt).toArray();
    sourceMongoAuthenticationDb = properties.getProperty("mongo.source.authentication.db");
    sourceMongoUsername = properties.getProperty("mongo.source.username");
    sourceMongoPassword = properties.getProperty("mongo.source.password");
    sourceMongoEnableSSL = Boolean.parseBoolean(properties.getProperty("mongo.source.enableSSL"));
    sourceMongoDb = properties.getProperty("mongo.source.db");
    sourceMongoConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.source.connection.pool.size"), 500);
    sourceTranslationsMongoDb = properties.getProperty("mongo.source.translations.db");

    //Mongo Destination
    destinationMongoHosts = properties.getProperty("mongo.destination.hosts").split(",");
    destinationMongoPorts = Arrays
        .stream(properties.getProperty("mongo.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationMongoAuthenticationDb = properties
        .getProperty("mongo.destination.authentication.db");
    destinationMongoUsername = properties.getProperty("mongo.destination.username");
    destinationMongoPassword = properties.getProperty("mongo.destination.password");
    destinationMongoEnableSSL = Boolean
        .parseBoolean(properties.getProperty("mongo.destination.enableSSL"));
    destinationMongoDb = properties.getProperty("mongo.destination.db");
    destinationMongoConnectionPoolSize = NumberUtils.toInt(properties.getProperty("mongo.destination.connection.pool.size"), 500);

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
