package eu.europeana.metis.remove.plugin;

import com.opencsv.CSVReader;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.remove.utils.Application;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemovePluginsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemovePluginsMain.class);

  private static final String FILE_FOR_PLUGIN_REMOVAL = "/home/jochen/Desktop/plugins_to_remove.csv";

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    try (final Application application = Application.initialize()) {
      final List<PluginToRemove> plugins = readFile();
      LOGGER.info("Removing {} plugins.", plugins.size());
      final WorkflowExecutionDao dao = new WorkflowExecutionDao(application.getDatastoreProvider());
      plugins.forEach(plugin -> deletePlugin(plugin, dao, application.getDatastoreProvider()));
      LOGGER.info("Done.");
    }
  }

  private static List<PluginToRemove> readFile() throws IOException {
    final List<PluginToRemove> result = new ArrayList<>();
    final Path path = Paths.get(FILE_FOR_PLUGIN_REMOVAL);
    try (final BufferedReader fileReader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      final CSVReader reader = new CSVReader(fileReader);
      LOGGER.warn("Skipping first line: [{}].", String.join(", ", reader.readNext()));
      for (String[] line : reader) {
        if (line.length != 3) {
          throw new IllegalStateException(
              "Problem in line " + result.size() + " with data [" + String.join(", ", line)
                  + "]: not expecting " + line.length + " columns.");
        }
        if (StringUtils.isBlank(line[0]) || StringUtils.isBlank(line[1])) {
          throw new IllegalStateException(
              "Problem in line " + result.size() + " with data [" + String.join(", ", line)
                  + "]: not expecting blank values.");
        }
        final PluginType pluginType = PluginType.getPluginTypeFromEnumName(line[2]);
        if (pluginType == null) {
          throw new IllegalStateException(
              "Problem in line " + result.size() + " with data [" + String.join(", ", line)
                  + "]: not expecting plugin type value.");
        }
        result.add(new PluginToRemove(line[0], line[1], pluginType));
      }
    }
    return result;
  }


  private static void deletePlugin(PluginToRemove pluginToRemove, WorkflowExecutionDao dao,
      MorphiaDatastoreProvider datastoreProvider) {

    // Get the execution.
    final WorkflowExecution execution = dao.getById(pluginToRemove.getExecutionId());
    if (execution == null) {
      LOGGER
          .error("Could not find workflow execution with ID {}.", pluginToRemove.getExecutionId());
      return;
    }

    // Get the plugin.
    final AbstractMetisPlugin plugin = execution
        .getMetisPluginWithType(pluginToRemove.getPluginType()).orElse(null);
    if (plugin == null) {
      LOGGER.error("Could not find plugin execution of type {} in workflow with ID {}.",
          pluginToRemove.getPluginType(), pluginToRemove.getExecutionId());
      return;
    }

    // Test the ID.
    if (!plugin.getId().equals(pluginToRemove.getPluginId())) {
      LOGGER.error("Could not find plugin execution with ID {} and type {} in workflow with ID {}.",
          pluginToRemove.getPluginId(), pluginToRemove.getPluginType(),
          pluginToRemove.getExecutionId());
      return;
    }

    // Test that there is no other plugin depending on this one. NOTE: disabling validation on one
    // of the queries to suppress warnings from Morphia.
    final Query<AbstractMetisPlugin> pluginQuery = datastoreProvider.getDatastore()
        .createQuery(AbstractMetisPlugin.class);
    pluginQuery.field("pluginMetadata.revisionNamePreviousPlugin").equal(plugin.getPluginType().name());
    pluginQuery.field("pluginMetadata.revisionTimestampPreviousPlugin").equal(plugin.getStartedDate());
    final Query<WorkflowExecution> query =
        datastoreProvider.getDatastore().createQuery(WorkflowExecution.class).disableValidation();
    query.field("metisPlugins").elemMatch(pluginQuery);
    final List<WorkflowExecution> resultList = query.asList(new FindOptions().limit(1));
    if (!resultList.isEmpty()) {
      LOGGER.error("Could not remove plugin execution with ID {} and type {} in workflow with ID "
              + "{}: there seems to be a successor of this plugin in workflow with ID {}.",
          pluginToRemove.getPluginId(), pluginToRemove.getPluginType(),
          pluginToRemove.getExecutionId(), resultList.get(0).getId().toString());
      return;
    }

    // Test that if the plugin is not the last one, the next plugin has another source set (meaning
    // that it is not implicitly a successor of the plugin to be removed).
    final int pluginIndex = IntStream.range(0, execution.getMetisPlugins().size())
        .filter(index -> execution.getMetisPlugins().get(index).getId().equals(plugin.getId()))
        .findFirst().orElseThrow(IllegalStateException::new);
    if (pluginIndex != execution.getMetisPlugins().size() - 1) {
      final AbstractMetisPlugin nextPlugin = execution.getMetisPlugins().get(pluginIndex+1);
      final boolean previousTimestampIsSetAndDifferent =
          nextPlugin.getPluginMetadata().getRevisionTimestampPreviousPlugin() != null && !nextPlugin
              .getPluginMetadata().getRevisionTimestampPreviousPlugin()
              .equals(plugin.getStartedDate());
      final boolean previousNameIsSetAndDifferent =
          nextPlugin.getPluginMetadata().getRevisionNamePreviousPlugin() != null && !nextPlugin
              .getPluginMetadata().getRevisionNamePreviousPlugin()
              .equals(plugin.getPluginType().name());
      if (!previousTimestampIsSetAndDifferent && !previousNameIsSetAndDifferent) {
        LOGGER.error("Could not remove plugin execution with ID {} and type {} in workflow with ID "
                + "{}: the next plugin in the workflow seems to be the successor of this plugin.",
            pluginToRemove.getPluginId(), pluginToRemove.getPluginType(),
            pluginToRemove.getExecutionId());
        return;
      }
    }

    LOGGER.info("SUCCESS: plugin execution with ID {} and type {} in workflow with ID {}.",
        pluginToRemove.getPluginId(), pluginToRemove.getPluginType(),
        pluginToRemove.getExecutionId());

    // Update workflow TODO see rules in Jira ticket.
  }
}
