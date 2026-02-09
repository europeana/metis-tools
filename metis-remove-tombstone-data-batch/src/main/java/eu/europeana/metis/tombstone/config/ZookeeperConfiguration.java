package eu.europeana.metis.tombstone.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "zookeeper")
public record ZookeeperConfiguration(String[] hosts,
                                     int[] ports,
                                     String chroot,
                                     String defaultCollection) {

}
