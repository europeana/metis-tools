package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;
import java.util.List;

public abstract class AbstractPluginIdentificationEngine {

  private static final String FILE_FOR_PLUGIN_REMOVAL = "/home/jochen/Desktop/plugins_to_remove.csv";
  private static final String FILE_FOR_REVISION_REMOVAL = "/home/jochen/Desktop/revisions_to_remove.csv";
  private static final String FILE_FOR_TASK_REMOVAL = "/home/jochen/Desktop/tasks_to_remove.csv";

  protected void discoverPlugins() throws IOException, TrustStoreConfigurationException {
    try (final Application application = Application.initialize()) {
      final AbstractPluginIdentification pluginDiscoverer = createPluginDiscoverer(application);
      final List<ExecutionPluginNode> plugins = pluginDiscoverer.discoverPlugins();
      OutputUtils.saveFileForPluginRemoval(plugins, FILE_FOR_PLUGIN_REMOVAL);
      OutputUtils.saveFileForRevisionRemoval(plugins, application.getProperties().ecloudProvider,
          FILE_FOR_REVISION_REMOVAL);
      OutputUtils.saveFileForTaskRemoval(plugins, FILE_FOR_TASK_REMOVAL);
    }
  }

  abstract AbstractPluginIdentification createPluginDiscoverer(Application application);
}
