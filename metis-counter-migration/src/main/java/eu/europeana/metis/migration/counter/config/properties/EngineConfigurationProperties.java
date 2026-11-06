package eu.europeana.metis.migration.counter.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Engine.
 * <p>
 * This is used for metis-sandbox and should be eventually be the source of truth for any engine configuration.
 */
@ConfigurationProperties(prefix = "engine")
public record EngineConfigurationProperties(
    String baseUrl
) {

}
