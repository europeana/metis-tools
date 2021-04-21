package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;

/**
 * @see DatasetSpecificIdentification for the details.
 */
public class DatasetSpecificIdentificationMain extends AbstractPluginIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new DatasetSpecificIdentificationMain().discoverPlugins();
  }

  @Override
  AbstractPluginIdentification createPluginDiscoverer(Application application) {
    return new DatasetSpecificIdentification(application.getDatastoreProvider());
  }
}
