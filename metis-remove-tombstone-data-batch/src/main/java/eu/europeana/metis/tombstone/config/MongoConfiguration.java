package eu.europeana.metis.tombstone.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mongo")
public record MongoConfiguration(String[] hosts,
                                 int[] ports,
                                 String authenthicationDB,
                                 String userName,
                                 String password,
                                 boolean enableSSL,
                                 String tombstoneDB,
                                 int connectionPoolSize) {

}
