package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;

/**
 * @see CanceledOrFailedIdentification for the details.
 */
public class CanceledOrFailedIdentificationMain extends AbstractPluginIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new CanceledOrFailedIdentificationMain().discoverPlugins();
  }

  @Override
  AbstractPluginIdentification createPluginDiscoverer(Application application) {
    return new CanceledOrFailedIdentification(application.getDatastoreProvider());
  }
}
