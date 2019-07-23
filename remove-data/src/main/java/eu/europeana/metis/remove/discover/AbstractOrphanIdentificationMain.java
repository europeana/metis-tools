package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

public abstract class AbstractOrphanIdentificationMain {

  private static final String FILE_FOR_PLUGIN_REMOVAL = "/home/jochen/Desktop/plugins_to_remove.csv";
  private static final String FILE_FOR_REVISION_REMOVAL = "/home/jochen/Desktop/revisions_to_remove.csv";
  private static final String FILE_FOR_TASK_REMOVAL = "/home/jochen/Desktop/tasks_to_remove.csv";

  protected void discoverOrphans() throws IOException, TrustStoreConfigurationException {
    try (final Application application = Application.initialize()) {
      final AbstractOrphanIdentification discoverOrphans = createOrphansDiscoverer(application);
      final List<ExecutionPluginNode> orphans = discoverOrphans.discoverOrphans();
      OutputUtils.saveFileForPluginRemoval(orphans, FILE_FOR_PLUGIN_REMOVAL);
      OutputUtils.saveFileForRevisionRemoval(orphans, application.getProperties().ecloudProvider,
          FILE_FOR_REVISION_REMOVAL);
      OutputUtils.saveFileForTaskRemoval(orphans, FILE_FOR_TASK_REMOVAL);
    }
  }

  abstract AbstractOrphanIdentification createOrphansDiscoverer(Application application);
}
