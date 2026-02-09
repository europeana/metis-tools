package eu.europeana.metis.tombstone.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "solr")
public record SolrConfiguration(String[] hosts) {

}
