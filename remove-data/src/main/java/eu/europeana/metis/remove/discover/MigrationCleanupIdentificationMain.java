package eu.europeana.metis.remove.discover;

import com.opencsv.CSVWriter;
import eu.europeana.metis.CommonStringValues;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.remove.discover.AbstractOrphanIdentification.DiscoveryMode;
import eu.europeana.metis.remove.utils.Application;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

public class MigrationCleanupIdentificationMain {

  private static final String FILE_FOR_PLUGIN_REMOVAL = "/home/jochen/Desktop/plugins_to_remove.csv";
  private static final String FILE_FOR_REVISION_REMOVAL = "/home/jochen/Desktop/revisions_to_remove.csv";
  private static final String FILE_FOR_TASK_REMOVAL = "/home/jochen/Desktop/tasks_to_remove.csv";

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    try (final Application application = Application.initialize()) {
      final AbstractOrphanIdentification discoverOrphans = new MigrationCleanupIdentification(
          application.getDatastoreProvider(), DiscoveryMode.DISCOVER_ONLY_CHILDLESS_ORPHANS);
      final List<ExecutionPluginNode> orphans = discoverOrphans.discoverOrphans();
      saveFileForPluginRemoval(orphans);
      saveFileForRevisionRemoval(orphans, application.getProperties().ecloudProvider);
      saveFileForTaskRemoval(orphans);
    }
  }

  private static void saveFileForPluginRemoval(List<ExecutionPluginNode> nodesToRemove)
      throws IOException {
    final Path path = Paths.get(FILE_FOR_PLUGIN_REMOVAL);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "executionId",
          "pluginId",
          "pluginType"
      });

      // Write records
      nodesToRemove.forEach(node ->
          writer.writeNext(new String[]{
              node.getExecution().getId().toString(),
              node.getPlugin().getId(),
              node.getPlugin().getPluginType().name()
          })
      );
    }
  }

  private static void saveFileForRevisionRemoval(List<ExecutionPluginNode> nodesToRemove,
      String providerId) throws IOException {
    final Path path = Paths.get(FILE_FOR_REVISION_REMOVAL);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "dataSetId",
          "providerId",
          "representationName",
          "revisionName",
          "revisionProviderId",
          "revisionTimestamp"
      });

      // Write records
      final DateFormat dateFormat = new SimpleDateFormat(CommonStringValues.DATE_FORMAT, Locale.US);
      nodesToRemove.forEach(node ->
          writer.writeNext(new String[]{
              node.getExecution().getEcloudDatasetId(),
              providerId,
              AbstractMetisPlugin.getRepresentationName(),
              node.getPlugin().getPluginType().name(),
              providerId,
              dateFormat.format(node.getPlugin().getStartedDate().getTime())
          })
      );
    }
  }

  private static void saveFileForTaskRemoval(List<ExecutionPluginNode> nodesToRemove)
      throws IOException {
    final Path path = Paths.get(FILE_FOR_TASK_REMOVAL);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "taskId"
      });

      // Write records
      nodesToRemove.stream().map(ExecutionPluginNode::getPlugin)
          .map(AbstractMetisPlugin::getExternalTaskId).filter(StringUtils::isNotBlank)
          .forEach(taskId -> writer.writeNext(new String[]{taskId}, false));
    }
  }
}
