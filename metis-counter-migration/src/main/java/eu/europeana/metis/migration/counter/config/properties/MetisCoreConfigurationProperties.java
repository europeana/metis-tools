package eu.europeana.metis.migration.counter.config.properties;

import eu.europeana.metis.core.engine.base.EngineType;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Metis Core.
 * <p>
 * This record encapsulates application-level configuration settings for the Metis Core service, which can be configured using
 * properties prefixed with "metis-core".
 */
@ConfigurationProperties(prefix = "metis-core")
public record MetisCoreConfigurationProperties(
    EngineType engineType,
    int maxConcurrentThreads,
    int dpsMonitorCheckIntervalInSeconds,
    int dpsConnectTimeoutInMilliseconds,
    int dpsReadTimeoutInMilliseconds,
    int failsafeMarginOfInactivityInSeconds,
    int workflowDispatchPeriodCheckInMilliseconds,
    int pollingTimeoutForCleaningCompletionServiceInMilliseconds,
    int userCacheClearIntervalInMinutes,
    int periodOfNoProcessedRecordsChangeInMinutes,
    int threadLimitThrottlingLevelWeak,
    int threadLimitThrottlingLevelMedium,
    int threadLimitThrottlingLevelStrong,
    String baseUrl,
    int maxServedExecutionListLength,
    int maxDepublishRecordIdsPerDataset,
    int linkCheckingDefaultSamplingSize,
    int solrCommitPeriodInMinutes,
    List<String> allowedCorsHosts
) {

}
