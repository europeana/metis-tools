package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.discover.AbstractOrphanIdentification.DiscoveryMode;
import eu.europeana.metis.remove.utils.Application;
import java.io.IOException;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

public class DatasetSpecificIdentificationMain extends AbstractOrphanIdentificationEngine {

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    new DatasetSpecificIdentificationMain().discoverOrphans();
  }

  @Override
  AbstractOrphanIdentification createOrphansDiscoverer(Application application) {
    return new DatasetSpecificIdentification(application.getDatastoreProvider(),
        DiscoveryMode.DISCOVER_ONLY_CHILDLESS_ORPHANS);
  }
}