package eu.europeana.metis.remove.discover;

import com.mongodb.DBCollection;
import com.opencsv.CSVWriter;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutionProgress;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as an engine for discovering orphans. Individual cleanup actions can extend this
 * class and provide the functionality to identify the orphans given an instance of {@link
 * ExecutionPluginForest}.
 */
abstract class AbstractOrphanIdentification {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOrphanIdentification.class);

  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  AbstractOrphanIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
  }

  void discoverOrphans(String outputFile) throws IOException {

    // Compute all iterations
    final List<List<ExecutionPluginNode>> iterations = new ArrayList<>();
    final Set<String> idsToSkip = new HashSet<>();
    while (true) {
      final List<ExecutionPluginNode> nodesToRemove = new ArrayList<>();
      getDatasetIds().stream().map(id -> discoverOrphans(id, idsToSkip)).flatMap(List::stream)
          .map(ExecutionPluginNode::getAllInOrderOfRemoval).forEach(nodesToRemove::addAll);
      if (nodesToRemove.isEmpty()) {
        break;
      }
      nodesToRemove.stream().map(ExecutionPluginNode::getId).forEach(idsToSkip::add);
      iterations.add(nodesToRemove);
    }

    // If there is nothing to do
    if (iterations.isEmpty()) {
      LOGGER.info("Nothing to do: no orphans/leaf nodes found.");
      return;
    }

    // Print all iterations to the log.
    final ToIntFunction<ExecutionPluginNode> recordCounter = node -> Optional
        .ofNullable(node.getPlugin()).map(AbstractMetisPlugin::getExecutionProgress)
        .map(ExecutionProgress::getProcessedRecords).orElse(0);
    LOGGER.info("{} iterations are needed:", iterations.size());
    for (int i = 0; i < iterations.size(); i++) {
      final List<ExecutionPluginNode> nodesToRemove = iterations.get(i);
      final long linkCheckingExecutions = nodesToRemove.stream().map(ExecutionPluginNode::getType)
          .filter(type -> type == PluginType.LINK_CHECKING).count();
      LOGGER.info(
          " ... Iteration {}: remove {} nodes ({} records). {} of these are link checking executions.",
          i, nodesToRemove.size(), nodesToRemove.stream().mapToInt(recordCounter).sum(),
          linkCheckingExecutions);
    }

    // Write just the first iteration to a CSV.
    saveOrphansToFile(iterations.get(0), outputFile);
  }

  private void saveOrphansToFile(List<ExecutionPluginNode> nodesToRemove, String outputFile)
      throws IOException {
    final Path path = Paths.get(outputFile);
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final CSVWriter writer = new CSVWriter(fileWriter)) {

      // Write header
      writer.writeNext(new String[]{
          "Execution ID",
          "Execution creation date",
          "Metis Dataset ID",
          "eCloud Dataset ID",
          "Plugin ID",
          "Plugin type",
          "Plugin status",
          "Plugin start date",
          "Plugin external task ID",
          "Processed records"
      });

      // Write records
      nodesToRemove.forEach(node ->
          writer.writeNext(new String[]{
              node.getExecution().getId().toString(),
              node.getExecution().getCreatedDate().toString(),
              node.getExecution().getDatasetId(),
              node.getExecution().getEcloudDatasetId(),
              node.getPlugin().getId(),
              node.getType().name(),
              node.getPlugin().getPluginStatus().name(),
              node.getPlugin().getStartedDate().toString(),
              node.getPlugin().getExternalTaskId(),
              node.getPlugin().getExecutionProgress() == null ? ""
                  : ("" + node.getPlugin().getExecutionProgress().getProcessedRecords())
          })
      );
    }
  }

  private List<ExecutionPluginNode> discoverOrphans(String datasetId, Set<String> idsToSkip) {

    // Creating the forest
    final ExecutionPluginForest forest;
    try {
      forest = new ExecutionPluginForest(getWorkflowExecutions(datasetId), idsToSkip);
    } catch (RuntimeException e) {
      LOGGER.warn("Problem with dataset {}.", datasetId, e);
      return Collections.emptyList();
    }

    // Identify the orphans
    return identifyOrphans(forest);
  }

  abstract List<ExecutionPluginNode> identifyOrphans(ExecutionPluginForest forest);

  private Set<String> getDatasetIds() {
    final DBCollection collection = morphiaDatastoreProvider.getDatastore()
        .getCollection(WorkflowExecution.class);
    final List<?> datasetIds = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> collection.distinct("datasetId"));
    return datasetIds.stream().map(entry -> (String) entry).collect(Collectors.toSet());
  }

  private List<WorkflowExecution> getWorkflowExecutions(String datasetId) {
    final Query<WorkflowExecution> query = morphiaDatastoreProvider.getDatastore()
        .createQuery(WorkflowExecution.class).field("datasetId").equal(datasetId);
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(query::asList);
  }
}
