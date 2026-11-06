package eu.europeana.metis.migration.counter.config;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * The type Properties holder.
 */
@Configuration
public class PropertiesHolder {

  /**
   * The constant STATISTICS_LOGS_MARKER.
   */
  public static final Marker STATISTICS_LOGS_MARKER = MarkerFactory.getMarker("STATISTICS_LOGS");

  //General parameters
  @Value("${min.parallel.datasets}")
  private int minParallelDatasets;
  @Value("${max.parallel.threads.per.dataset}")
  private int maxParallelThreadsPerDataset;
  @Value("${mode}")
  private String mode;
  @Value("${dataset.ids.to.process}")
  private String datasetIdsToProcess;

  //Metis Core Mongo
  @Value("${truststore.path}")
  private String truststorePath;
  @Value("${truststore.password}")
  private String truststorePassword;
  @Value("${mongo.metis.core.hosts}")
  private String metisCoreMongoHosts;
  @Value("${mongo.metis.core.port}")
  private String metisCoreMongoPorts;
  @Value("${mongo.metis.core.authentication.db}")
  private String metisCoreMongoAuthenticationDb;
  @Value("${mongo.metis.core.username}")
  private String metisCoreMongoUsername;
  @Value("${mongo.metis.core.password}")
  private String metisCoreMongoPassword;
  @Value("${mongo.metis.core.enableSSL}")
  private boolean metisCoreMongoEnableSSL;
  @Value("${mongo.metis.core.db}")
  private String metisCoreMongoDb;
  @Value("${mongo.metis.core.readPreference}")
  private String metisCoreMongoReadPreference;
  @Value("${mongo.metis.core.connection.pool.size:50}")
  private int metisCoreConnectionPoolSize;

  /**
   * Get metis core mongo hosts string [ ].
   *
   * @return the string [ ]
   */
  public String[] getMetisCoreMongoHosts() {
    return metisCoreMongoHosts.split(",");
  }

  /**
   * Gets min parallel datasets.
   *
   * @return the min parallel datasets
   */
  public int getMinParallelDatasets() {
    return minParallelDatasets;
  }

  /**
   * Gets max parallel threads per dataset.
   *
   * @return the max parallel threads per dataset
   */
  public int getMaxParallelThreadsPerDataset() {
    return maxParallelThreadsPerDataset;
  }

  /**
   * Gets mode.
   *
   * @return the mode
   */
  public Mode getMode() {
    return Mode.getModeFromEnumName(mode);
  }

  /**
   * Gets dataset ids to process.
   *
   * @return the dataset ids to process
   */
  public List<String> getDatasetIdsToProcess() {
    return Arrays.stream(datasetIdsToProcess.split(","))
                 .filter(StringUtils::isNotBlank).map(String::trim).toList();
  }

  /**
   * Gets truststore path.
   *
   * @return the truststore path
   */
  public String getTruststorePath() {
    return truststorePath;
  }

  /**
   * Gets truststore password.
   *
   * @return the truststore password
   */
  public String getTruststorePassword() {
    return truststorePassword;
  }

  /**
   * Get metis core mongo ports int [ ].
   *
   * @return the int [ ]
   */
  public int[] getMetisCoreMongoPorts() {
    if (StringUtils.isBlank(this.metisCoreMongoPorts)) {
      return new int[0];
    } else {
      return Arrays.stream(metisCoreMongoPorts.split(","))
                   .mapToInt(Integer::parseInt).toArray();
    }
  }

  /**
   * Gets metis core mongo authentication db.
   *
   * @return the metis core mongo authentication db
   */
  public String getMetisCoreMongoAuthenticationDb() {
    return metisCoreMongoAuthenticationDb;
  }

  /**
   * Gets metis core mongo username.
   *
   * @return the metis core mongo username
   */
  public String getMetisCoreMongoUsername() {
    return metisCoreMongoUsername;
  }

  /**
   * Gets metis core mongo password.
   *
   * @return the metis core mongo password
   */
  public String getMetisCoreMongoPassword() {
    return metisCoreMongoPassword;
  }

  /**
   * Is metis core mongo enable ssl boolean.
   *
   * @return the boolean
   */
  public boolean isMetisCoreMongoEnableSSL() {
    return metisCoreMongoEnableSSL;
  }

  /**
   * Gets metis core mongo db.
   *
   * @return the metis core mongo db
   */
  public String getMetisCoreMongoDb() {
    return metisCoreMongoDb;
  }

  /**
   * Gets metis core connection pool size.
   *
   * @return the metis core connection pool size
   */
  public int getMetisCoreConnectionPoolSize() {
    return metisCoreConnectionPoolSize;
  }

  /**
   * Gets metis core mongo read preference.
   *
   * @return the metis core mongo read preference
   */
  public String getMetisCoreMongoReadPreference() {
    return metisCoreMongoReadPreference;
  }
}
