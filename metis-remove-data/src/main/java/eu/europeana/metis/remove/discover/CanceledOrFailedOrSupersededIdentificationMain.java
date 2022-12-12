package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;

/**
 * @see CanceledOrFailedOrSupersededIdentification for the details.
 */
public class CanceledOrFailedOrSupersededIdentificationMain extends AbstractPluginIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new CanceledOrFailedOrSupersededIdentificationMain().discoverPlugins();
  }

  @Override
  AbstractPluginIdentification createPluginDiscoverer(Application application) {
    return new CanceledOrFailedOrSupersededIdentification(application.getDatastoreProvider());
  }
}
