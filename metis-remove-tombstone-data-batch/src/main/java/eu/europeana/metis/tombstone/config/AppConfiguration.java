package eu.europeana.metis.tombstone.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppConfiguration(String mode, int batchChunkSize) {

}
