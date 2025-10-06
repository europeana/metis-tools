package eu.europeana.metis.finder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "info.app")
public record InfoAppConfigurationProperties(
    String title,
    String version,
    String description,
    String repository,
    Contact contact
) {

  /**
   * Represents a contact entity containing details such as name, email, and URL.
   */
  public record Contact(String name, String email, String url) {

  }
}
