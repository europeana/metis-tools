package eu.europeana.metis.remove.discover;

import com.opencsv.CSVWriter;
import eu.europeana.metis.CommonStringValues;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
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

final class OutputUtils {

  private OutputUtils() {
  }

  static void saveFileForPluginRemoval(List<ExecutionPluginNode> nodesToRemove,
      String fileForPluginRemoval) throws IOException {
    final Path path = Paths.get(fileForPluginRemoval);
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

  static void saveFileForRevisionRemoval(List<ExecutionPluginNode> nodesToRemove,
      String providerId, String fileForRevisionRemoval) throws IOException {
    final Path path = Paths.get(fileForRevisionRemoval);
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
      nodesToRemove.stream().filter(node -> node.getPlugin() instanceof AbstractExecutablePlugin)
          .filter(node -> node.getPlugin().getStartedDate() != null)
          .forEach(node ->
              writer.writeNext(new String[]{
                  node.getExecution().getEcloudDatasetId(),
                  providerId,
                  AbstractMetisPlugin.getRepresentationName(),
                  node.getPlugin().getPluginType().name(),
                  providerId,
                  dateFormat.format(node.getPlugin().getStartedDate().getTime())
              }, false)
          );
    }
  }

  static void saveFileForTaskRemoval(List<ExecutionPluginNode> nodesToRemove,
      String fileForTaskRemoval) throws IOException {
    final Path path = Paths.get(fileForTaskRemoval);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "taskId"
      });

      // Write records
      nodesToRemove.stream().map(ExecutionPluginNode::getPlugin)
          .filter(plugin -> plugin instanceof AbstractExecutablePlugin)
          .map(plugin -> (AbstractExecutablePlugin) plugin)
          .map(AbstractExecutablePlugin::getExternalTaskId).filter(StringUtils::isNotBlank)
          .forEach(taskId -> writer.writeNext(new String[]{taskId}, false));
    }
  }
}
