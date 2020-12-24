package eu.europeana.metis.reprocessing.utilities;

import java.util.Properties;

/**
 * Extra properties class that is part of {@link PropertiesHolder}.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class PropertiesHolderExtension {

  public final String enrichmentUrl;
  public final String dereferenceUrl;

  PropertiesHolderExtension(Properties properties) {
    enrichmentUrl = properties.getProperty("enrichment.url");
    dereferenceUrl = properties.getProperty("dereference.url");
  }
}
