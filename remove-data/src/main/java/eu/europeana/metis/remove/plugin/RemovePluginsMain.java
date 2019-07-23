package eu.europeana.metis.remove.plugin;

import com.opencsv.CSVReader;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.remove.utils.Application;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemovePluginsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemovePluginsMain.class);

  private enum Mode {
    REMOVE_FROM_DB, MARK_AS_DELETED
  }

  private static final String FILE_FOR_PLUGIN_REMOVAL = "/home/jochen/Desktop/plugins_to_remove.csv";

  private static final Mode MODE = Mode.REMOVE_FROM_DB;

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    try (final Application application = Application.initialize()) {
      final List<PluginToRemove> plugins = readFile();
      LOGGER.info("Removing {} plugins.", plugins.size());
      final WorkflowExecutionDao dao = new WorkflowExecutionDao(application.getDatastoreProvider());
      plugins.stream().map(plugin -> RemovePluginsMain.findPlugin(plugin, dao))
          .filter(Objects::nonNull).filter(pluginPair -> canEditWorkflow(pluginPair.getLeft()))
          .filter(pluginPair -> canDeletePlugin(pluginPair, application.getDatastoreProvider()))
          .forEach(pluginPair -> deletePlugin(pluginPair, application.getDatastoreProvider()));
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
          throwExceptionForLine(result.size(), line, "not expecting " + line.length + " columns");
        }
        if (StringUtils.isBlank(line[0]) || StringUtils.isBlank(line[1])) {
          throwExceptionForLine(result.size(), line, "not expecting blank values");
        }
        final PluginType pluginType = PluginType.getPluginTypeFromEnumName(line[2]);
        if (pluginType == null) {
          throwExceptionForLine(result.size(), line, "not expecting plugin type value");
        }
        result.add(new PluginToRemove(line[0], line[1], pluginType));
      }
    }
    return result;
  }

  private static void throwExceptionForLine(int lineNumber, String[] line, String message) {
    final String exception = String.format("Problem in line %s with data [%s]: %s.", lineNumber,
        String.join(", ", line), message);
    throw new IllegalStateException(exception);
  }

  private static boolean canEditWorkflow(WorkflowExecution execution) {

    // Check whether the workflow has finished.
    if (execution.getWorkflowStatus() == WorkflowStatus.RUNNING ||
        execution.getWorkflowStatus() == WorkflowStatus.INQUEUE) {
      LOGGER.error(
          "Could not edit workflow execution with ID {}: it is currently in queue or running.",
          execution.getId());
      return false;
    }

    // Check whether all plugins in the workflow have finished
    final EnumSet processingStates = EnumSet
        .of(PluginStatus.RUNNING, PluginStatus.INQUEUE, PluginStatus.CLEANING,
            PluginStatus.PENDING);
    if (execution.getMetisPlugins().stream()
        .map(AbstractMetisPlugin::getPluginStatus).anyMatch(processingStates::contains)) {
      LOGGER.error(
          "Could not edit workflow execution with ID {}: one of the plugins is currently processing.",
          execution.getId());
      return false;
    }

    // No impediments.
    return true;
  }

  private static Pair<WorkflowExecution, AbstractMetisPlugin> findPlugin(
      PluginToRemove pluginToRemove, WorkflowExecutionDao dao) {

    // Get the execution.
    final WorkflowExecution execution = dao.getById(pluginToRemove.getExecutionId());
    if (execution == null) {
      LOGGER
          .error("Could not find workflow execution with ID {}.", pluginToRemove.getExecutionId());
      return null;
    }

    // Get the plugin.
    final AbstractMetisPlugin plugin = execution
        .getMetisPluginWithType(pluginToRemove.getPluginType()).orElse(null);
    if (plugin == null || !plugin.getId().equals(pluginToRemove.getPluginId())) {
      LOGGER.error("Could not find plugin execution with ID {} and type {} in workflow with ID {}.",
          pluginToRemove.getPluginId(), pluginToRemove.getPluginType(),
          pluginToRemove.getExecutionId());
      return null;
    }

    // Done
    return new ImmutablePair<>(execution, plugin);
  }

  private static boolean canDeletePlugin(
      Pair<WorkflowExecution, AbstractMetisPlugin> executionAndPlugin,
      MorphiaDatastoreProvider datastoreProvider) {
    final WorkflowExecution execution = executionAndPlugin.getLeft();
    final AbstractMetisPlugin plugin = executionAndPlugin.getRight();

    // Test that there is no other plugin depending on this one. NOTE: disabling validation on one
    // of the queries to suppress warnings from Morphia.
    final Query<AbstractMetisPlugin> pluginQuery = datastoreProvider.getDatastore()
        .createQuery(AbstractMetisPlugin.class).disableValidation();
    pluginQuery.field("pluginMetadata.revisionNamePreviousPlugin")
        .equal(plugin.getPluginType().name());
    pluginQuery.field("pluginMetadata.revisionTimestampPreviousPlugin")
        .equal(plugin.getStartedDate());
    final Query<WorkflowExecution> query =
        datastoreProvider.getDatastore().createQuery(WorkflowExecution.class).disableValidation();
    query.field("metisPlugins").elemMatch(pluginQuery);
    final List<WorkflowExecution> resultList = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> query.asList(new FindOptions().limit(1)));
    if (!resultList.isEmpty()) {
      LOGGER.error("Could not remove plugin execution with ID {} and type {} in workflow with ID "
              + "{}: there seems to be a successor of this plugin in workflow with ID {}.",
          plugin.getId(), plugin.getPluginType(), execution.getId(),
          resultList.get(0).getId().toString());
      return false;
    }

    // Test that if the plugin is not the last one, the next plugin has another source set (meaning
    // that it is not implicitly a successor of the plugin to be removed).
    final int pluginIndex = IntStream.range(0, execution.getMetisPlugins().size())
        .filter(index -> execution.getMetisPlugins().get(index).getId().equals(plugin.getId()))
        .findFirst().orElseThrow(IllegalStateException::new);
    if (pluginIndex != execution.getMetisPlugins().size() - 1) {
      final AbstractMetisPlugin nextPlugin = execution.getMetisPlugins().get(pluginIndex + 1);
      final boolean previousTimestampIsSetAndDifferent = nextPlugin.getPluginMetadata() != null
          && nextPlugin.getPluginMetadata().getRevisionTimestampPreviousPlugin() != null
          && !nextPlugin.getPluginMetadata().getRevisionTimestampPreviousPlugin()
          .equals(plugin.getStartedDate());
      final boolean previousNameIsSetAndDifferent = nextPlugin.getPluginMetadata() != null
          && nextPlugin.getPluginMetadata().getRevisionNamePreviousPlugin() != null && !nextPlugin
          .getPluginMetadata().getRevisionNamePreviousPlugin()
          .equals(plugin.getPluginType().name());
      if (!previousTimestampIsSetAndDifferent && !previousNameIsSetAndDifferent) {
        LOGGER.error("Could not remove plugin execution with ID {} and type {} in workflow with ID "
                + "{}: the next plugin in the workflow seems to be the successor of this plugin.",
            plugin.getId(), plugin.getPluginType(), execution.getId());
        return false;
      }
    }

    // So all is well.
    return true;
  }

  private static void deletePlugin(Pair<WorkflowExecution, AbstractMetisPlugin> executionAndPlugin,
      MorphiaDatastoreProvider datastoreProvider) {

    // If the mode calls for marking as deleted, we do this and are done.
    if (MODE == Mode.MARK_AS_DELETED) {
      if (executionAndPlugin.getRight() instanceof AbstractExecutablePlugin) {
        ((AbstractExecutablePlugin) executionAndPlugin.getRight()).setDataStatus(DataStatus.DELETED);
        ExternalRequestUtil.retryableExternalRequestConnectionReset(
            () -> datastoreProvider.getDatastore().save(executionAndPlugin.getLeft()));
      }
      return;
    }

    // If there is only one plugin, we remove the execution and we are done.
    final WorkflowExecution execution = executionAndPlugin.getLeft();
    if (execution.getMetisPlugins().size() == 1) {
      ExternalRequestUtil.retryableExternalRequestConnectionReset(
          () -> datastoreProvider.getDatastore().delete(execution));
      return;
    }

    // Otherwise find the index of the plugin in the executions and remove it.
    final AbstractMetisPlugin plugin = executionAndPlugin.getRight();
    final int pluginIndex = IntStream.range(0, execution.getMetisPlugins().size())
        .filter(index -> execution.getMetisPlugins().get(index).getId().equals(plugin.getId()))
        .findFirst().orElseThrow(IllegalStateException::new);
    final List<AbstractMetisPlugin> metisPlugins = new ArrayList<>(execution.getMetisPlugins());
    metisPlugins.remove(pluginIndex);
    execution.setMetisPlugins(metisPlugins);

    // Set the new workflow status. In case it changes to CANCELLED, leave cancelledBy blank.
    execution.setWorkflowStatus(determineWorkflowStatus(execution.getMetisPlugins()));

    // Save the new execution.
    ExternalRequestUtil.retryableExternalRequestConnectionReset(
        () -> datastoreProvider.getDatastore().save(execution));
  }

  private static WorkflowStatus determineWorkflowStatus(List<AbstractMetisPlugin> plugins) {

    // Find first plugin that did not finish successfully. If none found, the workflow finished too.
    final PluginStatus firstNonSuccessfulPluginStatus = plugins.stream()
        .map(AbstractMetisPlugin::getPluginStatus).filter(status -> status != PluginStatus.FINISHED)
        .findFirst().orElse(null);
    if (firstNonSuccessfulPluginStatus == null) {
      return WorkflowStatus.FINISHED;
    }

    // Depending on the plugin status, determine the workflow status.
    final WorkflowStatus result;
    switch (firstNonSuccessfulPluginStatus) {
      case CANCELLED:
        result = WorkflowStatus.CANCELLED;
        break;
      case FAILED:
        result = WorkflowStatus.FAILED;
        break;
      default:
        // We know it cannot be FINISHED or any of the still-processing statuses.
        throw new IllegalStateException("Unexpected state: " + firstNonSuccessfulPluginStatus);
    }
    return result;
  }
}
