package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;

/**
 * @see MigrationCleanupIdentification for the details.
 */
public class MigrationCleanupIdentificationMain extends AbstractPluginIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new MigrationCleanupIdentificationMain().discoverPlugins();
  }

  @Override
  AbstractPluginIdentification createPluginDiscoverer(Application application) {
    return new MigrationCleanupIdentification(application.getDatastoreProvider());
  }
}
