package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.reprocessing.model.Mode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains all properties that are required for execution.
 * <p>During construction will read properties from the specified file from the classpath.
 * Internally it holds {@link PropertiesHolder#propertiesHolderExtension} that should contain the
 * extra required properties per re-process operation.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class PropertiesHolder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesHolder.class);
  public static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");
  public static final Marker STATISTICS_LOGS_MARKER = MarkerFactory.getMarker("STATISTICS_LOGS");

  //General parameters
  public final int minParallelDatasets;
  public final int maxParallelThreadsPerDataset;
  public final int startFromDatasetIndex;
  public final int endAtDatasetIndex;
  public final int sourceMongoPageSize;
  public final Mode mode;
  public final String[] datasetIdsToProcess;
  public final boolean identityProcess;
  public final boolean clearDatabasesBeforeProcess;
  public final List<ExecutablePluginType> invalidatePluginTypes;
  public final ExecutablePluginType reprocessBasedOnPluginType;

  //Metis Core Mongo
  public final String truststorePath;
  public final String truststorePassword;
  public final String[] metisCoreMongoHosts;
  public final int[] metisCoreMongoPorts;
  public final String metisCoreMongoAuthenticationDb;
  public final String metisCoreMongoUsername;
  public final String metisCoreMongoPassword;
  public final boolean metisCoreMongoEnablessl;
  public final String metisCoreMongoDb;
  //Mongo Source
  public final String[] sourceMongoHosts;
  public final int[] sourceMongoPorts;
  public final String sourceMongoAuthenticationDb;
  public final String sourceMongoUsername;
  public final String sourceMongoPassword;
  public final boolean sourceMongoEnablessl;
  public final String sourceMongoDb;
  //Mongo Destination
  public final String[] destinationMongoHosts;
  public final int[] destinationMongoPorts;
  public final String destinationMongoAuthenticationDb;
  public final String destinationMongoUsername;
  public final String destinationMongoPassword;
  public final boolean destinationMongoEnablessl;
  public final String destinationMongoDb;
  //Solr/Zookeeper Destination
  public final String[] destinationSolrHosts;
  public final String[] destinationZookeeperHosts;
  public final int[] destinationZookeeperPorts;
  public final String destinationZookeeperChroot;
  public final String destinationZookeeperDefaultCollection;

  private final PropertiesHolderExtension propertiesHolderExtension;

  public PropertiesHolder(String configurationFileName) {
    Properties properties = new Properties();
    final URL resource = getClass().getClassLoader().getResource(configurationFileName);
    final String filePathInResources = resource == null ? null : resource.getFile();
    String filePath;
    if (filePathInResources != null && new File(filePathInResources).exists()) {
      LOGGER.info(EXECUTION_LOGS_MARKER, "Will try to load {} properties file", filePathInResources);
      filePath = filePathInResources;
    } else {
      LOGGER.info(EXECUTION_LOGS_MARKER,
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
    datasetIdsToProcess =
        StringUtils.isBlank(properties.getProperty("dataset.ids.to.process")) ? null
            : properties.getProperty("dataset.ids.to.process").split(",");
    identityProcess = Boolean.parseBoolean(properties.getProperty("identity.process"));
    clearDatabasesBeforeProcess = Boolean
        .parseBoolean(properties.getProperty("clear.databases.before.process"));
    invalidatePluginTypes = Arrays
        .stream(properties.getProperty("invalidate.plugin.types").split(","))
        .map(ExecutablePluginType::getPluginTypeFromEnumName).collect(Collectors.toList());
    reprocessBasedOnPluginType = ExecutablePluginType
        .getPluginTypeFromEnumName(properties.getProperty("reprocess.based.on.plugin.type"));

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
    metisCoreMongoEnablessl = Boolean
        .parseBoolean(properties.getProperty("mongo.metis.core.enableSSL"));
    metisCoreMongoDb = properties.getProperty("mongo.metis.core.db");

    //Mongo Source
    sourceMongoHosts = properties.getProperty("mongo.source.hosts").split(",");
    sourceMongoPorts = Arrays.stream(properties.getProperty("mongo.source.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    sourceMongoAuthenticationDb = properties.getProperty("mongo.source.authentication.db");
    sourceMongoUsername = properties.getProperty("mongo.source.username");
    sourceMongoPassword = properties.getProperty("mongo.source.password");
    sourceMongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.source.enableSSL"));
    sourceMongoDb = properties.getProperty("mongo.source.db");

    //Mongo Destination
    destinationMongoHosts = properties.getProperty("mongo.destination.hosts").split(",");
    destinationMongoPorts = Arrays
        .stream(properties.getProperty("mongo.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationMongoAuthenticationDb = properties
        .getProperty("mongo.destination.authentication.db");
    destinationMongoUsername = properties.getProperty("mongo.destination.username");
    destinationMongoPassword = properties.getProperty("mongo.destination.password");
    destinationMongoEnablessl = Boolean
        .parseBoolean(properties.getProperty("mongo.destination.enableSSL"));
    destinationMongoDb = properties.getProperty("mongo.destination.db");

    //Solr/Zookeeper Destination
    destinationSolrHosts = properties.getProperty("solr.destination.hosts").split(",");
    destinationZookeeperHosts = properties.getProperty("zookeeper.destination.hosts").split(",");
    destinationZookeeperPorts = Arrays
        .stream(properties.getProperty("zookeeper.destination.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    destinationZookeeperChroot = properties.getProperty("zookeeper.destination.chroot");
    destinationZookeeperDefaultCollection = properties
        .getProperty("zookeeper.destination.defaultCollection");

    this.propertiesHolderExtension = new PropertiesHolderExtension(properties);
  }

  public PropertiesHolderExtension getPropertiesHolderExtension() {
    return propertiesHolderExtension;
  }
}
