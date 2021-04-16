package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;

/**
 * @see DatasetSpecificIdentification for the details.
 */
public class DatasetSpecificIdentificationMain extends AbstractOrphanIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new DatasetSpecificIdentificationMain().discoverOrphans();
  }

  @Override
  AbstractOrphanIdentification createOrphansDiscoverer(Application application) {
    return new DatasetSpecificIdentification(application.getDatastoreProvider());
  }
}
