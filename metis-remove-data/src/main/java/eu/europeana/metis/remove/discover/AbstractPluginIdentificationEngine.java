package eu.europeana.metis.remove.discover;

import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;
import java.util.List;

public abstract class AbstractPluginIdentificationEngine {

  private static final String FOLDER_FOR_OUTPUT = "/home/jochen/Desktop/cleanup/iteration2/";
  private static final String FILE_FOR_PLUGIN_REMOVAL = FOLDER_FOR_OUTPUT + "plugins_to_remove.csv";
  private static final String FILE_FOR_REVISION_REMOVAL = FOLDER_FOR_OUTPUT + "revisions_to_remove.csv";
  private static final String FILE_FOR_TASK_REMOVAL = FOLDER_FOR_OUTPUT + "tasks_to_remove.csv";

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
