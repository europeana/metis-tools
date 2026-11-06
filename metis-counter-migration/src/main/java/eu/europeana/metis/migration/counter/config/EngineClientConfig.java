package eu.europeana.metis.migration.counter.config;


import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.metis.core.engine.base.EngineTask;
import eu.europeana.metis.core.engine.base.EngineTaskSettings;
import eu.europeana.metis.core.execution.EngineTaskMonitor;
import eu.europeana.metis.migration.counter.config.properties.EcloudConfigurationProperties;
import eu.europeana.metis.core.engine.base.EngineTaskClient;
import eu.europeana.metis.core.engine.ecloud.EcloudEngineDatasetRecordClient;
import eu.europeana.metis.core.engine.ecloud.EcloudEngineTaskClient;
import eu.europeana.metis.core.engine.ecloud.EcloudEngineTaskSettings;
import eu.europeana.metis.core.engine.sandbox.SandboxEngineTaskClient;
import eu.europeana.metis.core.engine.sandbox.SandboxEngineTaskSettings;
import eu.europeana.metis.migration.counter.config.properties.EngineConfigurationProperties;
import eu.europeana.metis.migration.counter.config.properties.MetisCoreConfigurationProperties;
import eu.europeana.metis.core.engine.base.EngineType;
import eu.europeana.metis.core.workflow.plugins.ThrottlingValues;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

/**
 * Configuration class responsible for providing clients and settings required for interacting with processing engines.
 * <p>
 * Determines which type of client should be initialized based on configuration properties.
 */
@Configuration
@Slf4j
@EnableConfigurationProperties({MetisCoreConfigurationProperties.class, EcloudConfigurationProperties.class,
    EngineConfigurationProperties.class})
public class EngineClientConfig {

  private DpsClient dpsClient;
  private DataSetServiceClient dataSetServiceClient;
  private RecordServiceClient recordServiceClient;
  private FileServiceClient fileServiceClient;
  private UISClient uisClient;

  /**
   * Creates and configures a {@link RestClient} instance based on the specified engine configuration properties.
   *
   * @param engineConfigurationProperties the configuration properties for the engine
   * @return a fully configured {@link RestClient} instance
   */
  @Bean
  public RestClient engineRestClient(EngineConfigurationProperties engineConfigurationProperties) {
    return RestClient.builder()
                     .baseUrl(engineConfigurationProperties.baseUrl())
                     .build();
  }

  /**
   * Retrieves the throttling values based on the provided configuration properties.
   *
   * @param metisCoreConfigurationProperties Configuration properties containing the throttling level settings.
   * @return An instance of ThrottlingValues initialized with the provided configuration properties.
   */
  @Bean
  public ThrottlingValues getThrottlingValues(MetisCoreConfigurationProperties metisCoreConfigurationProperties) {
    return new ThrottlingValues(metisCoreConfigurationProperties.threadLimitThrottlingLevelWeak(),
        metisCoreConfigurationProperties.threadLimitThrottlingLevelMedium(),
        metisCoreConfigurationProperties.threadLimitThrottlingLevelStrong());
  }

  /**
   * Configures and returns an instance of EngineTaskClient based on the engine type.
   * <p>
   * If the engine type is ECLOUD, initializes an EcloudEngineTaskClient; otherwise, initializes a MockEngineTaskClient.
   *
   * @param metisCoreConfigurationProperties the core configuration properties for the Metis engine.
   * @param ecloudConfigurationProperties the configuration properties specific to the ECLOUD engine.
   * @param engineConfigurationProperties the configuration properties specific to the engine.
   * @param throttlingValues the throttling values for managing concurrency levels.
   * @param restClient the RestClient instance used for making HTTP requests. Currently used for metis-sandbox api.
   * @return An instance of EngineTaskClient configured based on the specified properties and engine type.
   */
  @Bean(destroyMethod = "close")
  @SuppressWarnings("java:S1452") //This is acceptable and the only way (at the time of writing) that spring allows.
  public EngineTaskClient<?, ?> engineTaskClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties,
      EngineConfigurationProperties engineConfigurationProperties,
      ThrottlingValues throttlingValues,
      RestClient restClient
  ) {
    if (EngineType.ECLOUD.equals(metisCoreConfigurationProperties.engineType())) {
      log.info("Initializing DPS Engine Task Client");
      return ecloudEngineTaskClient(metisCoreConfigurationProperties, ecloudConfigurationProperties, throttlingValues);
    } else if (EngineType.SANDBOX.equals(metisCoreConfigurationProperties.engineType())) {
      return sandboxEngineTaskClient(metisCoreConfigurationProperties, engineConfigurationProperties, restClient,
          throttlingValues);
    } else {
      throw new IllegalArgumentException("Invalid engine type: " + metisCoreConfigurationProperties.engineType());
    }
  }

  private EngineTaskClient<?, ?> sandboxEngineTaskClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EngineConfigurationProperties engineConfigurationProperties,
      RestClient restClient, ThrottlingValues throttlingValues) {
    SandboxEngineTaskSettings sandboxEngineTaskSettings =
        new SandboxEngineTaskSettings(engineConfigurationProperties.baseUrl(), null,
            metisCoreConfigurationProperties.baseUrl(), throttlingValues);
    return new SandboxEngineTaskClient(sandboxEngineTaskSettings, restClient);
  }

  private EngineTaskClient<?, ?> ecloudEngineTaskClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties,
      ThrottlingValues throttlingValues) {

    dpsClient = dpsClient(metisCoreConfigurationProperties, ecloudConfigurationProperties);
    dataSetServiceClient = dataSetServiceClient(metisCoreConfigurationProperties, ecloudConfigurationProperties);
    recordServiceClient = recordServiceClient(metisCoreConfigurationProperties, ecloudConfigurationProperties);
    fileServiceClient = fileServiceClient(metisCoreConfigurationProperties, ecloudConfigurationProperties);
    uisClient = uisClient(metisCoreConfigurationProperties, ecloudConfigurationProperties);
    EcloudEngineTaskSettings ecloudEngineTaskSettings = new EcloudEngineTaskSettings(
        ecloudConfigurationProperties.getBaseUrl(),
        ecloudConfigurationProperties.getProvider(),
        metisCoreConfigurationProperties.baseUrl(),
        throttlingValues
    );
    final EcloudEngineDatasetRecordClient ecloudEngineDatasetRecordClient = new EcloudEngineDatasetRecordClient(
        dataSetServiceClient,
        recordServiceClient, fileServiceClient, uisClient);
    return new EcloudEngineTaskClient(dpsClient, ecloudEngineTaskSettings, ecloudEngineDatasetRecordClient);
  }

  private DpsClient dpsClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties) {
    return new DpsClient(
        ecloudConfigurationProperties.getDpsBaseUrl(),
        ecloudConfigurationProperties.getUsername(),
        ecloudConfigurationProperties.getPassword(),
        metisCoreConfigurationProperties.dpsConnectTimeoutInMilliseconds(),
        metisCoreConfigurationProperties.dpsReadTimeoutInMilliseconds());
  }

  private DataSetServiceClient dataSetServiceClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties) {
    return new DataSetServiceClient(
        ecloudConfigurationProperties.getBaseUrl(),
        ecloudConfigurationProperties.getUsername(),
        ecloudConfigurationProperties.getPassword(),
        metisCoreConfigurationProperties.dpsConnectTimeoutInMilliseconds(),
        metisCoreConfigurationProperties.dpsReadTimeoutInMilliseconds());
  }

  private RecordServiceClient recordServiceClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties) {
    return new RecordServiceClient(
        ecloudConfigurationProperties.getBaseUrl(),
        ecloudConfigurationProperties.getUsername(),
        ecloudConfigurationProperties.getPassword(),
        metisCoreConfigurationProperties.dpsConnectTimeoutInMilliseconds(),
        metisCoreConfigurationProperties.dpsReadTimeoutInMilliseconds());
  }

  private FileServiceClient fileServiceClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties) {
    return new FileServiceClient(
        ecloudConfigurationProperties.getBaseUrl(),
        ecloudConfigurationProperties.getUsername(),
        ecloudConfigurationProperties.getPassword(),
        metisCoreConfigurationProperties.dpsConnectTimeoutInMilliseconds(),
        metisCoreConfigurationProperties.dpsReadTimeoutInMilliseconds());
  }

  private UISClient uisClient(
      MetisCoreConfigurationProperties metisCoreConfigurationProperties,
      EcloudConfigurationProperties ecloudConfigurationProperties) {
    return new UISClient(
        ecloudConfigurationProperties.getBaseUrl(),
        ecloudConfigurationProperties.getUsername(),
        ecloudConfigurationProperties.getPassword(),
        metisCoreConfigurationProperties.dpsConnectTimeoutInMilliseconds(),
        metisCoreConfigurationProperties.dpsReadTimeoutInMilliseconds());
  }

  /**
   * Closes all initialized service clients, releasing their resources to prevent potential memory leaks. If a client is null, no
   * action is performed for that client.
   */
  @PreDestroy
  public void close() {
    if (dataSetServiceClient != null) {
      dataSetServiceClient.close();
    }
    if (recordServiceClient != null) {
      recordServiceClient.close();
    }
    if (fileServiceClient != null) {
      fileServiceClient.close();
    }
    if (dpsClient != null) {
      dpsClient.close();
    }
    if (uisClient != null) {
      uisClient.close();
    }
  }
}

